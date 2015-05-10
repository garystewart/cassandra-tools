package domain

/*   Contains all the structure of objects from OpsCenter

 */
import org.json4s._
import org.json4s.jackson.JsonMethods._

object CassandraYaml {
    def parseCassandraYaml (body: String) = {
        implicit val formats = DefaultFormats
        parse(body).extract[CassandraYaml]
    }
}

case class CassandraYaml (authenticator                     : Option[String],
                          authority                         : Option[String],
                          auto_snapshot                     : Option[Boolean],
                          cluster_name                      : Option[String],
                         //..
                          concurrent_reads                  : Option[Int],
                          concurrent_writes                 : Option[Int] ) {}


case class Login (sessionid: String) {}

object Login {
    def parseLogin (body: String) = {
        implicit val formats = DefaultFormats
        parse(body).extract[Login]
    }
}




/*** Cassandra.yaml
{
    "column_index_size_in_kb": 64,
    "commitlog_directory": "/var/lib/cassandra/commitlog",
    "commitlog_segment_size_in_mb": 32,
    "commitlog_sync": "periodic",
    "commitlog_sync_period_in_ms": 10000,
    "compaction_preheat_key_cache": true,
    "compaction_throughput_mb_per_sec": 16,
//    "concurrent_reads": 32,
//    "concurrent_writes": 32,
    "data_file_directories": [
        "/var/lib/cassandra/data"
    ],
    "dynamic_snitch_badness_threshold": 0.1,
    "dynamic_snitch_reset_interval_in_ms": 600000,
    "dynamic_snitch_update_interval_in_ms": 100,
    "encryption_options": {
        "algorithm": "SunX509",
        "cipher_suites": [
            "TLS_RSA_WITH_AES_128_CBC_SHA",
            "TLS_RSA_WITH_AES_256_CBC_SHA"
        ],
        "internode_encryption": "none",
        "keystore": "conf/.keystore",
        "keystore_password": "cassandra",
        "protocol": "TLS",
        "store_type": "JKS",
        "truststore": "conf/.truststore",
        "truststore_password": "cassandra"
    },
    "endpoint_snitch": "org.apache.cassandra.locator.SimpleSnitch",
    "flush_largest_memtables_at": 0.75,
    "hinted_handoff_enabled": true,
    "hinted_handoff_throttle_delay_in_ms": 1,
    "in_memory_compaction_limit_in_mb": 64,
    "incremental_backups": false,
    "index_interval": 128,
    "initial_token": null,
    "key_cache_save_period": 14400,
    "listen_address": "192.168.1.1",
    "max_hint_window_in_ms": 3600000,
    "memtable_flush_queue_size": 4,
    "multithreaded_compaction": false,
    "partitioner": "org.apache.cassandra.dht.RandomPartitioner",
    "populate_io_cache_on_flush": false,
    "reduce_cache_capacity_to": 0.6,
    "reduce_cache_sizes_at": 0.85,
    "request_scheduler": "org.apache.cassandra.scheduler.NoScheduler",
    "row_cache_provider": "SerializingCacheProvider",
    "row_cache_save_period": 0,
    "row_cache_size_in_mb": 0,
    "rpc_address": "0.0.0.0",
    "rpc_keepalive": true,
    "rpc_port": 9160,
    "rpc_server_type": "sync",
    "rpc_timeout_in_ms": 10000,
    "saved_caches_directory": "/var/lib/cassandra/saved_caches",
    "seed_provider": [
        {
            "class_name": "org.apache.cassandra.locator.SimpleSeedProvider",
            "parameters": [
                {
                    "seeds": "192.168.1.1, 192.168.1.2"
                }
            ]
        }
    ],
    "snapshot_before_compaction": false,
    "ssl_storage_port": 7001,
    "storage_port": 7000,
    "thrift_framed_transport_size_in_mb": 15,
    "thrift_max_message_length_in_mb": 16,
    "trickle_fsync": false,
    "trickle_fsync_interval_in_kb": 10240
}
  */



case class Node (node_ip: String, node_name: String) {}
case class Nodes (nodes: List[Node]) {}

object Nodes {
    def parseBody (body: String) = {
        implicit val formats = DefaultFormats
        parse(body).extract[Nodes]
    }
}

/*

{
  "node_ip": <value>,
  "node_name": <value>,
  "token": <value>,
  "node_version": <name:value>,
  "load": <value>,
  "data_held": <value>,
  "mode": <value>,
  "streaming": <name:value, name:value, . . .>,
  "task_progress": <name:value, name:value, . . .>,
  "last_seen": <value>,
  "num_procs": <value>,
  "rpc_ip": <value>,
  "dc": <value>,
  "rack": <value>,
  "network_interfaces": <value array>,
  "partitions": {
    "data": <value list>,
    "commitlog": <value>,
    "saved_caches": <value>,
    "other": <value list>
  },
  "devices": {
    "data": <value list>,
    "commitlog": <value>,
    "saved_caches": <value>,
    "other": <value list>
  },
  "os": <value>,
  "has_jna": <value>,
  "ec2": {
    "instance-id": <value>,
    "instance-type": <value>,
    "ami-id": <value>,
    "placement": <value>
  }
}
 */












//case class Cluster (jmx: String) {}
//case class ClusterConfig (name: Map[String, _]) {}
////TODO - Fix so that array works!
//object ClusterConfig {
//
//    def parseClusterConfig (body: String) = {
//        implicit val formats = DefaultFormats
//        println (parse(body) \ ".") //.extract[ClusterConfig]
//    }
//}


/*
{
  "Test_Cluster": {
    "cassandra": {
    "agents": {
      "use_ssl": "false"
    },
      "seed_hosts": "localhost"
    },
    "cassandra_metrics": {},
    "jmx": {
      "port": 7199
    }
  },
    "Test_Cluster2": {
      "cassandra" {
        "seed_hosts": "2.3.4.5, 2.3.4.6",
        "api_port": 9160
      },
      "cassandra_metrics": {},
      "jmx": {
        "port": 7199,
        "username": "jmx_user",
        "password": "jmx_pass"
      }
  }
}
 */

