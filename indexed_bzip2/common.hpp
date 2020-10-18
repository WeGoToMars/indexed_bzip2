#pragma once

#include <chrono>
#include <ostream>
#include <type_traits>
#include <utility>
#include <vector>


template<typename I1,
         typename I2,
         typename Enable = typename std::enable_if<
            std::is_integral<I1>::value &&
            std::is_integral<I2>::value
         >::type>
I1
ceilDiv( I1 dividend,
         I2 divisor )
{
    return ( dividend + divisor - 1 ) / divisor;
}


template<typename S, typename T>
std::ostream&
operator<<( std::ostream&  out,
            std::pair<S,T> pair )
{
    out << "(" << pair.first << "," << pair.second << ")";
    return out;
}


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


inline std::chrono::time_point<std::chrono::high_resolution_clock>
now()
{
    return std::chrono::high_resolution_clock::now();
}


/**
 * @return duration in seconds
 */
template<typename T0, typename T1>
double
duration( const T0& t0,
          const T1& t1 )
{
    return std::chrono::duration<double>( t1 - t0 ).count();
}
