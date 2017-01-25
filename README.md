# SPSC RingBuffer for futures_rs 

An implementation of a SPSC RingBuffer built on top of futures_rs.

## Example
`Cargo.toml`:
```toml
[dependencies]
futures = "0.1"
futures_rb = { git = "https://github.com/smith61/futures_rb" }
```

`main.rs`
```rust
extern crate futures;
extern crate futures_rb;

use futures::{
    Future
};
use futures_rb::rb;
use std::{
    str,
    thread
};

fn main( ) {
    let ( mut sender, mut receiver ) = rb::create_buffer( 1024 );
    
    thread::spawn( move | | {
        sender = sender.write_all( b"Hello World" ).wait( ).unwrap( );
    } );
    
    let mut buffer = [ 0; 1024 ];
    let ( bytes_read, mut receiver ) = receiver.read_some( &mut buffer ).wait( ).unwrap( );
    
    println!( "{}", str::from_utf8( &buffer[ .. bytes_read ] ).unwrap( ) );
}
```