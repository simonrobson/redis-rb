require "redis/connection/registry"
require "redis/connection/command_helper"
require "redis/errors"
require "socket"
require "celluloid/io"

class Redis
  module Connection

    class CelluloidTCPSocket < ::Celluloid::IO::TCPSocket
      include SocketMixin

      # def self.connect(host, port, timeout)
      #   Timeout.timeout(timeout) do
      #     sock = new(host, port)
      #     sock
      #   end
      # rescue Timeout::Error
      #   raise TimeoutError
      # end

      def self.connect(host, port, timeout)
        # Limit lookup to IPv4, as Redis doesn't yet do IPv6...
        addr = ::Socket.getaddrinfo(host, nil, Socket::AF_INET)
        sock = ::Socket.new(::Socket.const_get(addr[0][0]), Socket::SOCK_STREAM, 0)
        sockaddr = ::Socket.pack_sockaddr_in(port, addr[0][3])

        begin
          sock.connect_nonblock(sockaddr)
        rescue Errno::EINPROGRESS
          selector = NIO::Selector.new
          selector.register(sock, :rw)
          if selector.select(timeout).nil?
            raise TimeoutError
          end

          # if IO.select(nil, [sock], nil, timeout) == nil
          #   raise TimeoutError
          # end

          begin
            sock.connect_nonblock(sockaddr)
          rescue Errno::EISCONN
          end
        end

          sock
      end
    end

    class Celluloid
      include Redis::Connection::CommandHelper
      include ::Celluloid

      MINUS    = "-".freeze
      PLUS     = "+".freeze
      COLON    = ":".freeze
      DOLLAR   = "$".freeze
      ASTERISK = "*".freeze

      def self.connect(config)
        if config[:scheme] == "unix"
          # sock = ::Celluloid::IO::UNIXSocket.connect(config[:path], config[:timeout])
          sock = Celluloid::IO::UNIXSocket.new(config[:path])
        else
          # sock = Celluloid::IO::TCPSocket.connect(config[:host], config[:port], config[:timeout])
          sock = CelluloidTCPSocket.connect(config[:host], config[:port], config[:timeout])
        end

        instance = new(sock)
        instance.timeout = config[:timeout]
        instance
      end

      def initialize(sock)
        @sock = sock
      end

      def connected?
        !! @sock
      end

      def disconnect
        @sock.close
      rescue
      ensure
        @sock = nil
      end

      def timeout=(timeout)
        if @sock.respond_to?(:timeout=)
          @sock.timeout = timeout
        end
      end

      def write(command)
        @sock.write(build_command(command))
      end

      def read
        line = @sock.gets
        reply_type = line.slice!(0, 1)
        format_reply(reply_type, line)
      rescue Errno::EAGAIN
        raise TimeoutError
      end

      def format_reply(reply_type, line)
        case reply_type
        when MINUS    then format_error_reply(line)
        when PLUS     then format_status_reply(line)
        when COLON    then format_integer_reply(line)
        when DOLLAR   then format_bulk_reply(line)
        when ASTERISK then format_multi_bulk_reply(line)
        else raise ProtocolError.new(reply_type)
        end
      end

      def format_error_reply(line)
        CommandError.new(line.strip)
      end

      def format_status_reply(line)
        line.strip
      end

      def format_integer_reply(line)
        line.to_i
      end

      def format_bulk_reply(line)
        bulklen = line.to_i
        return if bulklen == -1
        reply = encode(@sock.read(bulklen))
        @sock.read(2) # Discard CRLF.
        reply
      end

      def format_multi_bulk_reply(line)
        n = line.to_i
        return if n == -1

        Array.new(n) { read }
      end

    end
  end
end

Redis::Connection.drivers << Redis::Connection::Celluloid
