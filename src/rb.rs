//! Module containing the implementation of a SPSC RingBuffer built on futures_rs.

use futures::{
    Async,
    Future,
    Poll,
    task
};
use std::{
    cmp
};
use std::sync::{
    Arc,
    Mutex
};

/// The sending end of the SPSC RingBuffer.
#[derive( Debug )]
pub struct Sender< T : Copy + Default > {
    inner : Arc< Mutex< RingBuffer< T > > >
}

/// Errors generated while sending elements to the RingBuffer.
#[derive( Debug )]
pub enum SendError {
    ReceiverDropped
}

/// Future that completes when at least one element has been written to the RingBuffer.
#[derive( Debug )]
pub struct WriteSome< 'a, T : Copy + Default + 'a > {
    sender : Option< Sender< T > >,
    buffer : &'a [ T ]
}

/// Future that completes when the entire buffer has been filled from the RingBuffer.
#[derive( Debug )]
pub struct WriteAll< 'a, T : Copy + Default + 'a > {
    sender        : Option< Sender< T > >,
    buffer        : &'a [ T ],
    elems_written : usize
}

/// The receiving end of the SPSC RingBuffer.
#[derive( Debug )]
pub struct Receiver< T : Copy + Default > {
    inner : Arc< Mutex< RingBuffer< T > > >
}

/// Errors generated when reading elements from the RingBuffer.
#[derive( Debug )]
pub enum ReadError {
    SenderDropped
}

/// Future that completes when at least one element has been read from the RingBuffer.
#[derive( Debug )]
pub struct ReadSome< 'a, T : Copy + Default + 'a > {
    receiver : Option< Receiver< T > >,
    buffer   : &'a mut [ T ]
}

/// Future that completes when the entire buffer has been read from the RingBuffer.
#[derive( Debug )]
pub struct ReadAll< 'a, T : Copy + Default + 'a > {
    receiver   : Option< Receiver< T > >,
    buffer     : &'a mut [ T ],
    elems_read : usize
}


#[derive( Debug )]
struct RingBuffer< T : Copy + Default > {
    buffer       : Vec< T >,
    read_offset  : usize,
    size         : usize,

    parked_read  : ParkedTask,
    parked_write : ParkedTask,

    buffer_state : RingBufferState
}

#[derive( Debug )]
enum RingBufferState {
    Open,
    ReceiverDropped,
    SenderDropped,
    Closed
}

#[derive( Debug )]
struct ParkedTask {
    task : Option< task::Task >
}

/// Creates a new RingBuffer with the given internal buffer size.
///
/// # Panics
/// If buffer_size == 0.
pub fn create_buffer< T : Copy + Default >( buffer_size : usize ) -> ( Sender< T >, Receiver< T > ) {
    let inner = Arc::new( Mutex::new( RingBuffer::new( buffer_size ) ) );

    ( Sender { inner : inner.clone( ) }, Receiver { inner : inner } )
}

impl < T : Copy + Default > Sender< T > {

    /// Attempts to write as many elements to the RingBuffer without blocking.
    ///
    /// # Errors
    /// If there is no Receiver.
    pub fn write( &mut self, buffer : &[ T ] ) -> Result< usize, SendError > {
        self.inner.lock( ).unwrap( ).write_some( buffer )
    }

    /// Attempts to write at least one element to the RingBuffer, returning a future
    /// that completes when at least one element has been written.
    pub fn write_some< 'a, E >( self, buffer : &'a [ T ] ) -> WriteSome< 'a, T > {
        WriteSome {
            sender : Some( self ),
            buffer : buffer
        }
    }

    /// Attempts to write the entire buffer to the RingBuffer, returning a future that
    /// completes when the entire buffer has been written.
    pub fn write_all< 'a >( self, buffer : &'a [ T ] ) -> WriteAll< 'a, T > {
        WriteAll {
            sender        : Some( self ),
            buffer        : buffer,
            elems_written : 0
        }
    }

    /// Tests to see if a call to write would write at least one element to the RingBuffer.
    ///
    /// If sender is not ready, the current task is parked and later unparked when the above is true.
    pub fn poll_write( &mut self ) -> Async< ( ) > {
        self.inner.lock( ).unwrap( ).poll_write( )
    }

}

impl < T : Copy + Default > Drop for Sender< T > {

    fn drop( &mut self ) {
        self.inner.lock( ).unwrap( ).close_sender( );
    }

}

impl < 'a, T : Copy + Default > Future for WriteSome< 'a, T > {

    type Item  = ( usize, Sender< T > );
    type Error = ( SendError, Sender< T > );


    fn poll( &mut self ) -> Poll< Self::Item, Self::Error > {
        if let Some( mut sender ) = self.sender.take( ) {
            if self.buffer.len( ) == 0 {
                return Ok( Async::Ready( ( 0, sender ) ) );
            }

            if let Async::NotReady = sender.poll_write( ) {
                return Ok( Async::NotReady );
            }

            let result = sender.write( self.buffer );
            match result {
                Ok( read ) => {
                    assert!( read != 0 );

                    Ok( Async::Ready( ( read, sender ) ) )
                },
                Err( error ) => {
                    Err( ( error, sender ) )
                }
            }
        }
        else {
            panic!( "Can not call poll on a completed future." );
        }
    }

}

impl < 'a, T : Copy + Default > Future for WriteAll< 'a, T > {

    type Item  = Sender< T >;
    type Error = ( SendError, Sender< T > );

    fn poll( &mut self ) -> Poll< Self::Item, Self::Error > {
        if let Some( mut sender ) = self.sender.take( ) {
            if self.buffer.len( ) == 0 {
                return Ok( Async::Ready( sender ) );
            }

            loop {
                if let Async::NotReady = sender.poll_write( ) {
                    self.sender = Some( sender );

                    return Ok( Async::NotReady );
                }

                let result = sender.write( &self.buffer[ self.elems_written .. ] );
                match result {
                    Ok( written ) => {
                        assert!( written != 0 );

                        self.elems_written += written;
                        if self.elems_written == self.buffer.len( ) {
                            return Ok( Async::Ready( sender ) );
                        }
                    },
                    Err( error ) => {
                        return Err( ( error, sender ) )
                    }
                }
            }
        }
        else {
            panic!( "Can not call poll on a completed future." );
        }
    }

}

impl < T : Copy + Default > Receiver< T > {

    /// Attempts to read as many elements from the RingBuffer as possible without blocking.
    ///
    /// # Errors
    /// If the buffer is empty and there is no Sender.
    pub fn read( &mut self, buffer : &mut [ T ] ) -> Result< usize, ReadError > {
        self.inner.lock( ).unwrap( ).read_some( buffer )
    }

    /// Attempts to read at least one element from the RingBuffer, returning a future that
    /// completes when at least one element has been read.
    pub fn read_some< 'a >( self, buffer : &'a mut [ T ] ) -> ReadSome< 'a, T >  {
        ReadSome {
            receiver : Some( self ),
            buffer   : buffer
        }
    }

    /// Attempts to read the entire buffer from the RingBuffer, returning a future that
    /// completes when at least the entire buffer has been read.
    pub fn read_all< 'a >( self, buffer : &'a mut [ T ] ) -> ReadAll< 'a, T > {
        ReadAll {
            receiver   : Some( self ),
            buffer     : buffer,
            elems_read : 0
        }
    }

    /// Tests to see if a call to read will read at least one element from the RingBuffer.
    ///
    /// If receiver is not ready, the current task is parked and later unparked when the above is true.
    pub fn poll_read( &mut self ) -> Async< ( ) > {
        self.inner.lock( ).unwrap( ).poll_read( )
    }

}

impl < T : Copy + Default > Drop for Receiver< T > {

    fn drop( &mut self ) {
        self.inner.lock( ).unwrap( ).close_receiver( );
    }

}

impl < 'a, T : Copy + Default + 'a > Future for ReadSome< 'a, T > {

    type Item  = ( usize, Receiver< T > );
    type Error = ( ReadError, Receiver< T > );

    fn poll( &mut self ) -> Poll< Self::Item, Self::Error > {
        if let Some( mut receiver ) = self.receiver.take( ) {
            if self.buffer.len( ) == 0 {
                return Ok( Async::Ready( ( 0, receiver ) ) );
            }

            if let Async::NotReady = receiver.poll_read( ) {
                self.receiver = Some( receiver );

                return Ok( Async::NotReady );
            }

            let result = receiver.read( self.buffer );
            match result {
                Ok( read ) => {
                    assert!( read != 0 );

                    Ok( Async::Ready( ( read, receiver ) ) )
                },
                Err( error ) => {
                    Err( ( error, receiver ) )
                }
            }
        }
        else {
            panic!( "Can not call poll on already completed future." );
        }
    }

}

impl < 'a, T : Copy + Default + 'a > Future for ReadAll< 'a, T > {

    type Item  = Receiver< T >;
    type Error = ( ReadError, Receiver< T > );

    fn poll( &mut self ) -> Poll< Self::Item, Self::Error > {
        if let Some( mut receiver ) = self.receiver.take( ) {
            if self.buffer.len( ) == 0 {
                return Ok( Async::Ready( receiver ) );
            }

            loop {
                if let Async::NotReady = receiver.poll_read( ) {
                    self.receiver = Some( receiver );

                    return Ok( Async::NotReady );
                }

                let result = receiver.read( &mut self.buffer[ self.elems_read .. ] );
                match result {
                    Ok( read ) => {
                        assert!( read != 0 );

                        self.elems_read += read;
                        if self.elems_read == self.buffer.len( ) {
                            return Ok( Async::Ready( receiver ) )
                        }
                    },
                    Err( error ) => {
                        return Err( ( error, receiver ) )
                    }
                }
            }
        }
        else {
            panic!( "Can not call poll on already completed future." );
        }
    }

}

impl < T : Copy + Default > RingBuffer< T > {

    fn new( buffer_size : usize ) -> Self {
        if buffer_size == 0 {
            panic!( "Can not create ring buffer with size == 0." );
        }

        RingBuffer {
            buffer       : vec![ T::default( ); buffer_size ],
            read_offset  : 0,
            size         : 0,

            parked_read  : ParkedTask::new( ),
            parked_write : ParkedTask::new( ),

            buffer_state : RingBufferState::Open
        }
    }

    fn available_slots( &self ) -> usize {
        self.buffer.len( ) - self.size
    }

    fn taken_slots( &self ) -> usize {
        self.size
    }

    fn write_some( &mut self, buffer : &[ T ] ) -> Result< usize, SendError > {
        if !self.has_receiver( ) {
            return Err( SendError::ReceiverDropped );
        }

        let buffer_size = self.buffer.len( );
        let write_ptr   = ( self.read_offset + self.size ) % buffer_size;

        let elems_to_write = cmp::min( buffer.len( ), self.available_slots( ) );

        let head_elems = cmp::min( elems_to_write, buffer_size - write_ptr );
        let tail_elems = elems_to_write - head_elems;


        self.buffer[ write_ptr .. write_ptr + head_elems ].copy_from_slice( &buffer[ .. head_elems ] );
        self.buffer[ .. tail_elems ].copy_from_slice( &buffer[ head_elems .. elems_to_write ] );

        self.size += elems_to_write;
        self.parked_read.unpark( );

        Ok( elems_to_write )
    }

    fn read_some( &mut self, buffer : &mut [ T ] ) -> Result< usize, ReadError > {
        if self.taken_slots( ) == 0 && !self.has_sender( ) {
            return Err( ReadError::SenderDropped );
        }

        let buffer_size = self.buffer.len( );
        let read_ptr    = self.read_offset;

        let elems_to_read = cmp::min( buffer.len( ), self.taken_slots( ) );

        let head_elems = cmp::min( elems_to_read, buffer_size - read_ptr );
        let tail_elems = elems_to_read - head_elems;

        buffer[ .. head_elems ].copy_from_slice( &self.buffer[ read_ptr .. read_ptr + head_elems ] );
        buffer[ head_elems .. elems_to_read ].copy_from_slice( &self.buffer[ .. tail_elems ] );

        self.read_offset = ( read_ptr + elems_to_read ) % buffer_size;
        self.size -= elems_to_read;
        self.parked_write.unpark( );

        Ok( elems_to_read )
    }

    fn poll_read( &mut self ) -> Async< ( ) > {
        if self.taken_slots( ) == 0 && self.has_sender( ) {
            self.parked_read.park( );

            Async::NotReady
        }
        else {
            Async::Ready( ( ) )
        }
    }

    fn poll_write( &mut self ) -> Async< ( ) > {
        if self.available_slots( ) == 0 && self.has_receiver( ) {
            self.parked_write.park( );

            Async::NotReady
        }
        else {
            Async::Ready( ( ) )
        }
    }

    fn has_sender( &self ) -> bool {
        match self.buffer_state {
            RingBufferState::Open | RingBufferState::ReceiverDropped => true,
            _ => false
        }
    }

    fn close_sender( &mut self ) {
        self.buffer_state = match self.buffer_state {
            RingBufferState::Open => RingBufferState::SenderDropped,
            RingBufferState::ReceiverDropped => RingBufferState::Closed,
            _ => unreachable!( )
        };
        self.parked_read.unpark( );
    }

    fn has_receiver( &self ) -> bool {
        match self.buffer_state {
            RingBufferState::Open | RingBufferState::SenderDropped => true,
            _ => false
        }
    }

    fn close_receiver( &mut self ) {
        self.buffer_state = match self.buffer_state {
            RingBufferState::Open => RingBufferState::ReceiverDropped,
            RingBufferState::SenderDropped => RingBufferState::Closed,
            _ => unreachable!( )
        };
        self.parked_write.unpark( )
    }

}

impl ParkedTask {

    fn new( ) -> Self {
        ParkedTask {
            task : None
        }
    }

    fn park( &mut self ) {
        let task = task::park( );

        self.task = Some( task );
    }

    fn unpark( &mut self ) {
        if let Some( parked_task ) = self.task.take( ) {
            parked_task.unpark( );
        }
    }

}