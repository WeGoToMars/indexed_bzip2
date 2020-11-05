#include <cassert>
#include <cstdlib>
#include <iostream>
#include <map>
#include <stdexcept>
#include <sstream>
#include <string>
#include <vector>

#include <cxxopts.hpp>

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
    cxxopts::Options options( "ibzip2",
                              "A bzip2 decompressor tool based on the indexed_bzip2 backend from ratarmount" );
    options.add_options( "Decompression" )
        ( "c,stdout"     , "Output to standard output. This is the default, when reading from standard input." )
        ( "d,decompress" , "Force decompression. Only for compatibility. No compression supported anyway." )
        ( "f,force"      , "Force overwriting existing output file and actual decoding even if piped to /dev/null." )
        ( "i,input"      , "Input file. Is none is given, data is read from standard input.",
          cxxopts::value<std::vector<std::string> >() )
        ( "k,keep"       , "Keep (do not delete) input file." )
        ( "t,test"       , "Test compressed file integrity." )

        ( "p,block-finder-parallelism",
          "This only has an effect if the parallel decoder is used with the -p option. "
          "If an optional >= 1 is given, then these are the number of threads to use for finding bzip2 blocks. ",
          cxxopts::value<unsigned int>()->default_value( "1" )->implicit_value( "0" ) )

        ( "P,decoder-parallelism",
          "Use the parallel decoder. "
          "If an optional >= 1 is given, then these are the number of decoder threads to use. "
          "Note that there might be further threads being started with non-decoding work.",
          cxxopts::value<unsigned int>()->default_value( "1" )->implicit_value( "0" ) );

    options.add_options( "Output" )
        ( "h,help"   , "Print this help mesage." )
        ( "q,quiet"  , "Suppress noncritical error messages." )
        ( "v,verbose", "Be verbose. A second -v (or shorthand -vv) gives even more verbosity." )
        ( "V,version", "Display software version." )
        ( "l,list-compressed-offsets",
          "List only the bzip2 block offsets given in bits one per line to the specified output file.",
          cxxopts::value<std::vector<std::string> >() )
        ( "L,list-offsets",
          "List bzip2 block offsets in bits and also the corresponding offsets in the decoded data at the beginning "
          "of each block in bytes as a comma separated pair per line '<encoded bits>,<decoded bytes>'." );

    options.parse_positional( { "input" } );

    const auto parsedArgs = options.parse( argc, argv );

    if ( parsedArgs.count( "help" ) > 0 ) {
        std::cout
        << options.help()
        << "\n"
        << "If no file names are given, ibzip2 decompresses from standard input to standard output.\n"
        << "If the output is discarded by piping to /dev/null, then the actual decoding step might\n"
        << "be omitted if neither --test nor -l nor -L nor --force are given."
        << std::endl;
        return 0;
    }

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
        const auto magicBytes = bitreader.read( 32 );

        std::stringstream msg;
        msg << it->first / 8 << " B " << it->first % 8 << " b : "  << it->second / 8 << " B " << " -> magic bytes: 0x"
            << std::hex << magicBytes << std::dec;
        // std::cerr msg.str() << "\n";

        if ( ( magicBytes != ( ( bzip2::MAGIC_BITS_BLOCK >> 16 ) & ~uint32_t( 0 ) ) ) &&
             ( magicBytes != ( ( bzip2::MAGIC_BITS_EOS   >> 16 ) & ~uint32_t( 0 ) ) ) ) {
            msg << " -> Found wrong offset in encoded data!\n";
            throw std::logic_error( msg.str() );
        }
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
