
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

#[derive( Debug )]
pub struct Sender< T : Copy + Default > {
    inner : Arc< Mutex< RingBuffer< T > > >
}

#[derive( Debug )]
pub enum SendError {
    ReceiverDropped
}

#[derive( Debug )]
pub struct WriteSome< 'a, T : Copy + Default + 'a > {
    sender : Option< Sender< T > >,
    buffer : &'a [ T ]
}

#[derive( Debug )]
pub struct WriteAll< 'a, T : Copy + Default + 'a > {
    sender        : Option< Sender< T > >,
    buffer        : &'a [ T ],
    elems_written : usize
}


#[derive( Debug )]
pub struct Receiver< T : Copy + Default > {
    inner : Arc< Mutex< RingBuffer< T > > >
}

#[derive( Debug )]
pub enum ReadError {
    SenderDropped
}

#[derive( Debug )]
pub struct ReadSome< 'a, T : Copy + Default + 'a > {
    receiver : Option< Receiver< T > >,
    buffer   : &'a mut [ T ]
}

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

pub fn create_buffer< T : Copy + Default >( buffer_size : usize ) -> ( Sender< T >, Receiver< T > ) {
    let inner = Arc::new( Mutex::new( RingBuffer::new( buffer_size ) ) );

    ( Sender { inner : inner.clone( ) }, Receiver { inner : inner } )
}

impl < T : Copy + Default > Sender< T > {

    pub fn write( &mut self, buffer : &[ T ] ) -> Result< usize, SendError > {
        self.inner.lock( ).unwrap( ).write_some( buffer )
    }

    pub fn write_some< 'a, E >( self, buffer : &'a [ T ] ) -> WriteSome< 'a, T > {
        WriteSome {
            sender : Some( self ),
            buffer : buffer
        }
    }

    pub fn write_all< 'a >( self, buffer : &'a [ T ] ) -> WriteAll< 'a, T > {
        WriteAll {
            sender        : Some( self ),
            buffer        : buffer,
            elems_written : 0
        }
    }

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

    pub fn read( &mut self, buffer : &mut [ T ] ) -> Result< usize, ReadError > {
        self.inner.lock( ).unwrap( ).read_some( buffer )
    }

    pub fn read_some< 'a >( self, buffer : &'a mut [ T ] ) -> ReadSome< 'a, T >  {
        ReadSome {
            receiver : Some( self ),
            buffer   : buffer
        }
    }

    pub fn read_all< 'a >( self, buffer : &'a mut [ T ] ) -> ReadAll< 'a, T > {
        ReadAll {
            receiver   : Some( self ),
            buffer     : buffer,
            elems_read : 0
        }
    }

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