#include <cassert>
#include <cstdio>
#include <ifstream>
#include <iostream>
#include <stdexcept>

#include "BitStringFinder.hpp"


namespace {
int gnTests = 0;
int gnTestErrors = 0;

/**
 * head -c $(( 100*1024*1024 )) /dev/urandom > decoded-sample
 * pbzip2 -k -c decoded-sample > encoded-sample.bz2
 */
constexpr std::string_view encodedTestFilePath = "encoded-sample.bz2";
constexpr std::string_view decodedTestFilePath = "decoded-sample";


template<typename T>
requireEqual( const T& a, const T& b, int line )
{
    ++gnTests;
    if ( a != b ) {
        ++gnTestErrors;
        std::cerr << "[FAIL on line " << line << "] " << a << " != " << b << "\n";
    }
}


std::ios_base::seekdir
toSeekdir( int origin )
{
    switch ( origin )
    {
    case SEEK_SET: return std::ios_base::beg;
    case SEEK_CUR: return std::ios_base::cur;
    case SEEK_END: return std::ios_base::end;
    }

    throw std::invalid_argument( "Unknown origin" );
}
}


#define REQUIRE_EQUAL( a, b ) requireEqual( a, b, __LINE__ )


template<typename T>
std::ostream&
operator<<( std::ostream&  out,
            std::vector<T> vector )
{
    out << "{ ";
    for ( const auto value : vector ) {
        out << value << ", ";
    }
    out << " }";
    return out;
}


/**
 * Tests are in such a way that seeking and reading are mirrored on the ParallelBZ2Reader file and the decoded file.
 * Then all read results can be checked against each other. Same for the result of tell.
 */
void
testDecodingBz2ForFirstTime()
{
    std::ifstream     decodedFile( decodedTestFilePath );
    ParallelBZ2Reader encodedFile( encodedTestFilePath );

    const auto seek =
        [&]( long long int offset,
             int           origin = SEEK_SET )
        {
            const auto newSeekPosDecoded = decodedFile.seek( offset, toSeekdir( origin ) );
            const auto newSeekPosEncoded = encodedFile.seek( offset, origin );
            REQUIRE_EQUAL( newSeekPosDecoded, newSeekPosEncoded );
            REQUIRE_EQUAL( decodedFile.tell(), encodedFile.tell() );
            REQUIRE_EQUAL( decodedFile.eof(), encodedFile.eof() );
        };

    const auto read =
        [&]( size_t nBytesToRead )
        {
            std::vector<char> decodedBuffer( nBytesToRead, 111 );
            std::vector<char> encodedBuffer( nBytesToRead, 333 );
            const auto decodedBytesCount = decodedFile.read( -1, decodedBuffer.data(), nBytesToRead );
            const auto encodedBytesCount = encodedFile.read( -1, encodedBuffer.data(), nBytesToRead );
            REQUIRE_EQUAL( decodedBytesCount, encodedBytesCount );
            REQUIRE_EQUAL( decodedFile.tell(), encodedFile.tell() );
            REQUIRE_EQUAL( decodedFile.eof(), encodedFile.eof() );
            REQUIRE_EQUAL( decodedBuffer, encodedBuffer );
        };

    /* Try some subsequent reads over bz2 block boundaries */

}


int
main( void )
{
    testDecodingBz2ForFirstTime();
    /* Sequential Reads on Unknown File */

    /* Seek Forwards on Unknown File */

    /* Seek Backwards on Unknown File */

    /*

    std::cout << "Tests successful: " << ( gnTests - gnTestErrors ) << " / " << gnTests << "\n";

    return gnTestErrors;
}
