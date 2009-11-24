require 'socket'
module Replication
  DEFAULT_PORT = 3463
  NUM_THREADS  = 1

  ACK_POLICY = {
    :all       => Bdb::DB_REPMGR_ACKS_ALL,
    :all_peers => Bdb::DB_REPMGR_ACKS_ALL_PEERS,
    :none      => Bdb::DB_REPMGR_ACKS_NONE,
    :one       => Bdb::DB_REPMGR_ACKS_ONE,
    :one_peer  => Bdb::DB_REPMGR_ACKS_ONE_PEER,
    :quorom    => Bdb::DB_REPMGR_ACKS_QUORUM,
  }

  def replicate?
    not @replicate.nil?
  end

  def master?
    not replicate? or replicate[:master]
  end

  def replicate(opts = nil)
    return @replicate if opts.nil?

    master  = normalize_host(opts.delete(:from))
    clients = [*opts.delete(:to)].compact.collect {|h| normalize_host(h)}
    local   = normalize_host(opts.delete(:host) || ENV['BDB_REPLICATION_HOST'], opts.delete(:port))
    remote  = clients + [master] - [local]

    opts[:master] = (local == master)
    opts[:local]  = local
    opts[:remote] = remote
    opts[:num_threads] ||= NUM_THREADS
    @replicate = opts

    env
  end

private

  def init_replication(env)
    env.set_verbose(Bdb::DB_VERB_REPLICATION, true) if replicate[:verbose]
    env.rep_priority = replicate[:master] ? 1 : 0
    env.repmgr_ack_policy = ACK_POLICY[replicate[:ack_policy]] if replicate[:ack_policy]
    env.repmgr_set_local_site(*replicate[:local])
    replicate[:remote].each do |s|
      env.repmgr_add_remote_site(*s)
    end
    env.rep_nsites = replicate[:remote].size + 1
  end

  def start_replication(env)
    env.repmgr_start(replicate[:num_threads], replicate[:master] ? Bdb::DB_REP_MASTER : Bdb::DB_REP_CLIENT)
  end

  def normalize_host(*host)
    host = host.compact.join(':')
    host, port = host.split(':')
    host ||= Socket.gethostname
    port ||= DEFAULT_PORT
    port = port.to_i

    addr_info = Socket.getaddrinfo(host.strip, port)
    ip = addr_info.detect {|i| i[0] == 'AF_INET'}[3]
    [ip, port]
  end
end
