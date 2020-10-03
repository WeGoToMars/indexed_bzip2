#pragma once

#include <chrono>
#include <cmath>
#include <limits>
#include <mutex>
#include <optional>
#include <queue>
#include <stdexcept>


/**
 * A FIFO container with an associated mutex which bundles locking and even waiting accesses.
 */
template<class T>
class ThreadSafeQueue
{
public:
    ThreadSafeQueue() = default;

    void
    push( const T& value )
    {
        std::lock_guard<std::mutex> lock( m_mutex );
        m_queue.emplace( value );
        m_changed.notify_all();
    }

    void
    push( T&& value )
    {
        std::lock_guard<std::mutex> lock( m_mutex );
        m_queue.emplace( std::move( value ) );
        m_changed.notify_all();
    }

    /**
     * @param timeoutSeconds Can be specified to wait until new data arrives from another thread.
     *                       To wait indefinitely specify std::numeric_limits<double>::infinity().
     */
    std::optional<T>
    pop( double timeoutSeconds = 0 )
    {
        if ( std::isnan( timeoutSeconds ) || ( timeoutSeconds < 0. ) ) {
            throw std::invalid_argument( "Time must be a non-negative number!" );
        }

        std::unique_lock<std::mutex> lock( m_mutex );
        if ( timeoutSeconds == std::numeric_limits<double>::infinity() ) {
            m_changed.wait( lock, [this](){ return !m_queue.empty(); } );
        } else {
            const auto timeout = std::chrono::nanoseconds( static_cast<size_t>( timeoutSeconds * 1e9 ) );
            m_changed.wait_for( lock, timeout, [this](){ return !m_queue.empty(); } );
        }

        if ( m_queue.empty() ) {
            return std::nullopt;
        }

        std::optional<T> result( m_queue.front() );
        m_queue.pop();
        return result;
    }

    [[nodiscard]] bool
    empty() const
    {
        return m_queue.empty();
    }

    [[nodiscard]] size_t
    size() const
    {
        return m_queue.size();
    }

private:
    std::queue<T> m_queue;
    std::mutex m_mutex;
    std::condition_variable m_changed;
};
