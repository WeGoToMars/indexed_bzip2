#include <cassert>
#include <cstdlib>
#include <iostream>
#include <map>
#include <stdexcept>
#include <sstream>
#include <string>
#include <vector>

#include <BitReader.hpp>
#include <ParallelBZ2Reader.hpp>


template<typename T1, typename T2>
std::ostream&
operator<<( std::ostream& out, std::map<T1,T2> data )
{
    for ( auto it = data.begin(); it != data.end(); ++it ) {
        out << "  " << it->first << " : " << it->second << "\n";
    }
    return out;
}


int main( int argc, char** argv )
{
    if ( argc < 2 ) {
        std::cerr << "A bzip2 file name to decompress must be specified!\n";
        return 1;
    }
    const std::string filename ( argv[1] );
    const int bufferSize = argc > 2 ? std::atoi( argv[2] ) : 0;

    std::cerr << "Concurrency: " << std::thread::hardware_concurrency() << "\n";

    ParallelBZ2Reader reader( filename );
    size_t nBytesWrittenTotal = 0;
    if ( bufferSize > 0 ) {
        do {
            std::vector<char> buffer( bufferSize, 0 );
            const size_t nBytesRead = reader.read( -1, buffer.data(), buffer.size() );
            assert( nBytesRead <= buffer.size() );
            const auto nBytesWritten = write( STDOUT_FILENO, buffer.data(), nBytesRead );
            nBytesWrittenTotal += nBytesRead;
        } while ( !reader.eof() );
    } else {
        nBytesWrittenTotal = reader.read( STDOUT_FILENO );
    }
    const auto offsets = reader.blockOffsets();
    //reader.seek( 900000 );

    BitReader bitreader( filename );

    //std::cerr << "Calculated CRC : 0x" << std::hex << reader.crc() << std::dec << "\n";
    std::cerr << "Stream size written: " << nBytesWrittenTotal << " B\n";
    std::cerr << "Stream size as reported: " << reader.size() << " B\n";
    std::cerr << "Block offsets  :\n";
    for ( auto it = offsets.begin(); it != offsets.end(); ++it ) {
        bitreader.seek( it->first );
        std::cerr
        << it->first / 8 << " B " << it->first % 8 << " b : "  << it->second / 8 << " B " << " -> magic bytes: 0x"
        << std::hex << bitreader.read( 32 ) << std::dec << "\n";
    }
    std::cerr << "Found " << offsets.size() << " blocks\n";

    if ( nBytesWrittenTotal != reader.size() ) {
        std::stringstream msg;
        msg << "Wrote less bytes (" << nBytesWrittenTotal << " B) than decoded stream is large("
            << reader.size() << " B)!";
        throw std::logic_error( msg.str() );
    }

    return 0;
}
