extern crate mio;
extern crate bytes;
#[macro_use]
extern crate log;
extern crate env_logger;
extern crate getopts;
extern crate net2;

use std::mem;
use std::net::SocketAddr;
use std::io;
use std::env;
use std::time::Duration;
use std::thread::sleep;
use std::collections::HashMap;

use mio::{TryRead, TryWrite, Evented, Token, EventSet, PollOpt};
use mio::tcp::*;
use mio::util::Slab;
use bytes::{Buf, MutBuf, RingBuf};
use getopts::Options;

use net2::TcpBuilder;
use net2::unix::UnixTcpBuilderExt;

const LISTENER: Token = Token(0);
const HUB: Token = Token(1);

const MAX_CLIENTS: usize = 8192;
const WRITE_BUFFER: usize = 65536;
const READ_BUFFER: usize = 16384; // Max line/command length

struct LineStream {
    stream: TcpStream,
    rbuf: RingBuf,
    wbuf: RingBuf,
}

impl LineStream {
    fn new(stream: TcpStream, rmax: usize, wmax: usize) -> LineStream {
        LineStream {
            stream: stream,
            rbuf: RingBuf::new(rmax),
            wbuf: RingBuf::new(wmax),
        }
    }
    
    fn push_data(&mut self) -> io::Result<Option<usize>> {
        self.stream.try_write_buf(&mut self.wbuf)
    }

    fn pull_data(&mut self) -> io::Result<Option<usize>> {
        self.stream.try_read_buf(&mut self.rbuf)
    }
    
    fn can_write(&self, len:usize) -> bool {
        MutBuf::remaining(&self.wbuf) >= len
    }
    
    fn write(&mut self, buf: &[u8]) -> Result<(), ()> {
        if MutBuf::remaining(&self.wbuf) < buf.len() {
            Err(())
        } else {
            self.wbuf.write_slice(buf);
            Ok(())
        }
    }
    
    fn readline(&mut self) -> Option<Vec<u8>> {
        self.rbuf.mark();
        let mut ret = Vec::new();
        while let Some(byte) = self.rbuf.read_byte() {
            ret.push(byte);
            if byte == 10 {
                return Some(ret);
            }
        }
        if ret.len() >= (self.rbuf.capacity() - 1) {
            Some(ret)
        } else {
            self.rbuf.reset();
            None
        }
    }
    
    fn rollback(&mut self) {
        self.rbuf.reset();
    }
        
    fn reregister(&self, event_loop: &mut mio::EventLoop<Proxy>, token: mio::Token) -> io::Result<()> {
        let mut events = EventSet::none();
        if ! self.rbuf.is_full() {
            events = events | EventSet::readable()
        }
        if ! self.wbuf.is_empty() {
            events = events | EventSet::writable()
        }
        event_loop.reregister(&self.stream, token,
            events,
            PollOpt::level())
    }

    fn deregister(&self, event_loop: &mut mio::EventLoop<Proxy>) -> io::Result<()> {
        event_loop.deregister(&self.stream)
    }
}

#[derive(PartialEq)]
#[derive(Debug)]
enum ProxyState {
    Idle,
    Connecting,
    Active,
}

struct Proxy {
    listener: TcpListener,
    hub_address: SocketAddr,
    hub: Option<LineStream>,
    connections: Slab<Connection>,
    state: ProxyState,
    pending_client: Option<(TcpStream, SocketAddr)>,
    pause_listener: bool,
    sid_map: HashMap<Vec<u8>, Token>,
    cmd_queue: Vec<Vec<u8>>,
    disconnect_queue: Vec<Token>,
    clients_stalled: bool,
}

impl Proxy {
    fn new(listener: TcpListener, hub_address: SocketAddr) -> Proxy {
        let slab = Slab::new_starting_at(Token(2), MAX_CLIENTS);

        Proxy {
            listener: listener,
            hub_address: hub_address,
            hub: None,
            connections: slab,
            state: ProxyState::Idle,
            pending_client: None,
            pause_listener: false,
            sid_map: HashMap::new(),
            cmd_queue: Vec::new(),
            disconnect_queue: Vec::new(),
            clients_stalled: false,
        }
    }
    
    fn connect(&mut self, event_loop: &mut mio::EventLoop<Proxy>) {
        let stream = TcpStream::connect(&self.hub_address).unwrap();

        event_loop.register(&stream, HUB, EventSet::writable() | EventSet::error(),
            PollOpt::level()).unwrap();

        self.hub = Some(LineStream::new(stream, READ_BUFFER, WRITE_BUFFER));
        self.state = ProxyState::Connecting;
    }
    
    fn cleanup(&mut self, event_loop: &mut mio::EventLoop<Proxy>) {
        if let Some(ref stream) = self.hub {
            stream.deregister(event_loop).unwrap();
        }
        self.hub = None;
        self.pending_client = None;

        self.state = ProxyState::Idle;
        self.connections.clear();
        self.sid_map.clear();
        self.cmd_queue.clear();
        self.reregister_listener(event_loop);
    }
    
    fn send_commands(&mut self)
    {
        while ! self.cmd_queue.is_empty() {
            if self.hub.as_mut().unwrap().write(self.cmd_queue[0].as_slice()).is_err() {
                error!("[HUB] Hub is backlogged, deferring management commands!");
                return
            }
            debug!("[HUB] < {:?}", std::str::from_utf8(self.cmd_queue[0].as_slice()));
            self.cmd_queue.remove(0);
        }
    }
    
    fn add_client(&mut self, stream: TcpStream, addr: SocketAddr)
    {
        let cmd = format!("+ {}\n", addr.ip()).into_bytes();
        self.cmd_queue.push(cmd);
        self.send_commands();
        self.pending_client = Some((stream, addr));
    }
    
    fn remove_client(&mut self, sid: &[u8])
    {
        let mut cmd = b"- ".to_vec();
        cmd.extend_from_slice(sid);
        cmd.push(b'\n');
        self.cmd_queue.push(cmd);
        self.send_commands();
    }

    fn reregister_listener(&mut self, event_loop: &mut mio::EventLoop<Proxy>) {
        debug!("reregister_listener state={:?} pause={:?} pending={:?} remaining={:?}",
            self.state, self.pause_listener, self.pending_client, self.connections);
        let events = if (self.state == ProxyState::Active) &&
                        (!self.pause_listener) &&
                        self.pending_client.is_none() &&
                        self.connections.has_remaining() {
            debug!("Listener is ACTIVE");
            EventSet::readable()
        } else {
            debug!("Listener is INACTIVE");
            EventSet::none()
        };
        
        event_loop.reregister(&self.listener, LISTENER,
            events, PollOpt::level()).unwrap();
    }

    fn reregister_hub(&mut self, event_loop: &mut mio::EventLoop<Proxy>) {
        self.hub.as_mut().unwrap().reregister(event_loop, HUB).unwrap();
    }
    
    fn handle_hub_message(&mut self, event_loop: &mut mio::EventLoop<Proxy>, msg: Vec<u8>) {
        debug!("[HUB] > {:?}", std::str::from_utf8(msg.as_slice()));
        
        if msg.len() < 3  || msg[msg.len() - 1] != b'\n' {
            error!("[HUB] Truncated message! Ignoring: {:?}", std::str::from_utf8(msg.as_slice()));
            return;
        }
        
        let args = &msg[2..];
        
        match msg[0] {
            b'+' => {
                self.msg_client_connected(event_loop, &args[..args.len() - 1])
            }
            b'-' => {
                self.msg_client_disconnect(event_loop, &args[..args.len() - 1])
            }
            b'B' => {
                self.msg_broadcast(event_loop, args)
            }
            b'M' => {
                self.msg_unicast(event_loop, args)
            }
            _ => {
                error!("Unknown message from hub: {:?}", std::str::from_utf8(msg.as_slice()));
            }
        }
    }
    
    fn msg_client_connected(&mut self, event_loop: &mut mio::EventLoop<Proxy>, args: &[u8]) {
        let (stream, addr) = mem::replace(&mut self.pending_client, None).unwrap();
        
        let token = self.connections
            .insert_with(|token| Connection::new(stream, token, args.to_vec()))
            .unwrap();

        info!("[{:?}] Client connection {} is established as sid {:?}",
            token, addr, std::str::from_utf8(args));

        if let Some(&oldtoken) = self.sid_map.get(&args.to_vec()) {
            error!("[{:?}] New SID {:?} already registered to token {:?}, killing old client",
            token, std::str::from_utf8(args), oldtoken);
            self.sid_map.remove(&args.to_vec());
            self.disconnect_queue.push(oldtoken);
        }
        self.sid_map.insert(args.to_vec(), token);

        event_loop.register(
            &self.connections[token].stream.stream,
            token,
            EventSet::readable(),
            PollOpt::edge() | PollOpt::oneshot()).unwrap();
            
        self.reregister_listener(event_loop);
    }

    fn msg_client_disconnect(&mut self, _: &mut mio::EventLoop<Proxy>, args: &[u8]) {
        if let Some(&token) = self.sid_map.get(&args.to_vec()) {
            info!("[{:?}] Queuing disconection for {:?}", token, std::str::from_utf8(args));
            self.sid_map.remove(&args.to_vec());
            self.disconnect_queue.push(token);
        } else {
            error!("Unknown SID disconnect request: {:?}", std::str::from_utf8(args));
        }
    }

    fn process_client_disconnect(&mut self, event_loop: &mut mio::EventLoop<Proxy>, token: Token) {
        info!("[{:?}] Processing disconection", token);
        self.connections[token].close(event_loop);
        self.connections.remove(token);
        self.reregister_listener(event_loop);
        self.pause_listener = false;
        self.reregister_hub(event_loop);
    }

    fn msg_unicast(&mut self, event_loop: &mut mio::EventLoop<Proxy>, args: &[u8]) {
        let mut idx = 0;
        while idx < args.len() && args[idx] != b' ' {
            idx = idx + 1;
        }
        if idx >= args.len() {
            error!("Bad arguments");
            return;
        }
        let sid = &args[..idx];
        let msg = &args[idx + 1..];
        
        if let Some(&token) = self.sid_map.get(sid) {
            if {
                let conn = &mut self.connections[token];
                if conn.closed {
                    error!("[{:?}] Client is already closed", conn.token);
                    return;
                } 
                if conn.stream.write(msg).is_err() {
                    error!("[{:?}] Client fell behind, killing it", conn.token);
                    conn.close(event_loop);
                    true
                } else {
                    conn.reregister(event_loop);
                    conn.closed
                }
            } {
                self.remove_client(&sid);
            }
        } else {
            error!("Unknown SID: {:?}", std::str::from_utf8(args));
        }
        
    }

    fn msg_broadcast(&mut self, event_loop: &mut mio::EventLoop<Proxy>, args: &[u8]) {
        let mut dead_clients = Vec::new();
        for conn in self.connections.iter_mut() {
            if conn.closed {
                continue
            }
            if conn.stream.write(args).is_err() {
                error!("[{:?}] Client fell behind, killing it", conn.token);
                dead_clients.push(conn.sid.clone());
                conn.close(event_loop);
            }
            conn.reregister(event_loop);
            if conn.closed {
                dead_clients.push(conn.sid.clone());
            }
        }
        for sid in dead_clients {
            self.remove_client(&sid.as_slice());
        }
    }
}

impl mio::Handler for Proxy {
    type Timeout = ();
    type Message = ();

    fn ready(&mut self, event_loop: &mut mio::EventLoop<Proxy>, token: Token, events: EventSet) {
        //debug!("socket is ready; token={:?}; events={:?}", token, events);

        if self.state == ProxyState::Idle {
            error!("Spurious events ignored: token={:?}; events={:?}", token, events);
            return;
        }

        match token {
            LISTENER => {
                assert!(events.is_readable());
                
                if ! self.connections.has_remaining() {
                    error!("[LISTENER] Client limit reached! Ceasing to accept new connections");
                    self.reregister_listener(event_loop);
                    return;
                }

                match self.listener.accept() {
                    Ok(Some((socket, addr))) => {
                        info!("[LISTENER] new client from {}", addr);
                        self.add_client(socket, addr);
                        self.reregister_hub(event_loop);
                    }
                    Ok(None) => {
                        info!("[LISTENER] spurious wakeup");
                    }
                    Err(e) => {
                        error!("[LISTENER] accept() returned an error: {:?}", e);
                        // This could be a fd limit or something. Deregister the
                        // event listener. It will be reregistered when the next
                        // client disconnect occurs, which will hopefully happen
                        // soon.
                        self.pause_listener = true;
                    }
                }
                self.reregister_listener(event_loop);
            }
            HUB => {
                if events.is_error() {
                    error!("[HUB] Upstream connection failed. Retrying...");
                    self.cleanup(event_loop);
                    sleep(Duration::from_secs(1));
                    return;
                }
                match self.state {
                    ProxyState::Idle => { unreachable!(); }
                    ProxyState::Connecting => {
                        assert!(events.is_writable());
                        error!("[HUB] Upstream connection established");
                        self.state = ProxyState::Active;
                        event_loop.reregister(&self.listener, LISTENER,
                            EventSet::readable(), PollOpt::level()).unwrap();
                        self.hub.as_mut().unwrap().write(b"MUX0\n").unwrap();
                        self.reregister_hub(event_loop);
                    }
                    ProxyState::Active => {
                        if events.is_readable() {
                            match self.hub.as_mut().unwrap().pull_data() {
                                Ok(Some(0)) => {
                                    error!("[HUB] Upstream connection closed. Disconnecting...");
                                    self.cleanup(event_loop);
                                    return;
                                }
                                Err(e) => {
                                    error!("[HUB] Upstream read failed with error {}. Disconnecting...", e);
                                    self.cleanup(event_loop);
                                    return;
                                }
                                _ => {}
                            }
                            while let Some(line) = self.hub.as_mut().unwrap().readline() {
                                self.handle_hub_message(event_loop, line);
                            }
                        }
                        if events.is_writable() {
                            if let Err(e) = self.hub.as_mut().unwrap().push_data() {
                                error!("[HUB] Upstream write failed with error {}. Disconnecting...", e);
                                self.cleanup(event_loop);
                                return;
                            }
                            self.send_commands();
                            if self.clients_stalled {
                                // TODO make this less dumb
                                self.clients_stalled = false;
                                for conn in self.connections.iter_mut() {
                                    if conn.stalled {
                                        conn.forward_to_hub(self.hub.as_mut().unwrap());
                                        conn.reregister(event_loop);
                                        if conn.stalled {
                                            self.clients_stalled = true;
                                        }
                                    }
                                }
                            }
                        }
                        self.reregister_hub(event_loop);
                    }
                }
            }
            _ => {
                if self.connections.get(token).is_none() {
                    error!("[{:?}] Unknown token, ignoring!", token);
                    return;
                }
                self.connections[token].ready(event_loop, events, self.hub.as_mut().unwrap());
                if self.connections[token].closed {
                    let sid = self.connections[token].sid.clone();
                    self.remove_client(sid.as_slice());
                } else if self.connections[token].stalled {
                    error!("[HUB] Client token {:?} is stalled on hub", token);
                    self.clients_stalled = true;
                }
                self.reregister_hub(event_loop);
            }
        }
    }

    fn tick(&mut self, event_loop: &mut mio::EventLoop<Proxy>) {
        if self.state == ProxyState::Idle {
            error!("[HUB] Event processing complete and proxy is idle, reconnecting...");
            self.connect(event_loop);
        }
        let queue = mem::replace(&mut self.disconnect_queue, Vec::new());
        for token in queue.iter().cloned() {
            self.process_client_disconnect(event_loop, token);
        }
    }

}

struct Connection {
    stream: LineStream,
    token: Token,
    sid: Vec<u8>,
    closed: bool,
    stalled: bool,
}

impl Connection {
    fn new(socket: TcpStream, token: Token, sid: Vec<u8>) -> Connection {
        Connection {
            stream: LineStream::new(socket, READ_BUFFER, WRITE_BUFFER),
            token: token,
            sid: sid,
            closed: false,
            stalled: false,
        }
    }

    fn ready(&mut self, event_loop: &mut mio::EventLoop<Proxy>, events: EventSet, hub:&mut LineStream) {
        if events.is_readable() {
            match self.stream.pull_data() {
                Ok(Some(0)) => {
                    info!("[{:?}] Connection closed.", self.token);
                    self.close(event_loop);
                    return;
                }
                Err(e) => {
                    info!("[{:?}] Read failed with error {}.", self.token, e);
                    self.close(event_loop);
                    return;
                }
                _ => {}
            }
            self.forward_to_hub(hub);
        }
        if events.is_writable() {
            if let Err(e) = self.stream.push_data() {
                info!("[{:?}] Write failed with error {}.", self.token, e);
                self.close(event_loop);
                return;
            }
        }
        self.reregister(event_loop);
    }
    
    fn close(&mut self, event_loop: &mut mio::EventLoop<Proxy>) {
        if ! self.closed {
            let _ = event_loop.deregister(&self.stream.stream);
            self.closed = true;
        }
    }
    
    fn reregister(&mut self, event_loop: &mut mio::EventLoop<Proxy>) {
        if ! self.closed {
            if let Err(e) = self.stream.reregister(event_loop, self.token) {
                error!("[{:?}] Registration failed with error {}.", self.token, e);
                self.close(event_loop);
            }
        }
    }
    
    
    fn forward_to_hub(&mut self, hub:&mut LineStream) {
        while let Some(line) = self.stream.readline() {
            debug!("[{:?}] > {:?}", self.token, std::str::from_utf8(line.as_slice()));
            if line[line.len()-1] != b'\n' {
                error!("[{:?}] Message is truncated, ignoring", self.token);
                continue;
            }
            if ! hub.can_write(3 + self.sid.len() + line.len()) {
                self.stream.rollback();
                self.stalled = true;
                return;
            } else {
                hub.write(b"M ").unwrap();
                hub.write(self.sid.as_slice()).unwrap();
                hub.write(b" ").unwrap();
                hub.write(line.as_slice()).unwrap();
            }
        }
        self.stalled = false;
    }
}

pub fn start(address: SocketAddr, upstream: SocketAddr) {
    let sock = (match address {
            SocketAddr::V4(..) => TcpBuilder::new_v4(),
            SocketAddr::V6(..) => TcpBuilder::new_v6(),
    }).unwrap();

    sock.reuse_address(true).unwrap();
    sock.reuse_port(true).unwrap();
    sock.bind(address).unwrap();

    let listener = TcpListener::from_listener(sock.listen(1024).unwrap(), &address).unwrap();
    //let listener = TcpListener::bind(&address).unwrap();

    let mut event_loop = mio::EventLoop::new().unwrap();
    // Register the listener but do not poll for events until we are
    // connected upstream.
    event_loop.register(&listener, LISTENER, EventSet::none(),
        PollOpt::level()).unwrap();
    
    let mut proxy = Proxy::new(listener, upstream);
    proxy.connect(&mut event_loop);
    event_loop.run(&mut proxy).unwrap();
}

fn print_usage(program: &str, opts: Options) {
    let brief = format!("Usage: {} [options]", program);
    print!("{}", opts.usage(&brief));
}

pub fn main() {
    env_logger::init().unwrap();

    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optopt("l", "listen", "listen on this host:port", "HOSTPORT");
    opts.optopt("u", "upstream", "connec to this upstream host:port", "HOSTPORT");
    opts.optflag("h", "help", "print this help menu");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => { m }
        Err(f) => { panic!(f.to_string()) }
    };
    if matches.opt_present("h") || !matches.opt_present("u") || !matches.opt_present("l") {
        print_usage(&program, opts);
        return;
    }
    let listen = matches.opt_str("l").unwrap();
    let hub = matches.opt_str("u").unwrap();
    start(listen.parse().unwrap(), hub.parse().unwrap());
}
