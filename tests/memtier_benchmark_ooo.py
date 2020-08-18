import tempfile
import json
from include import *
from mbdirector.benchmark import Benchmark
from mbdirector.runner import RunConfig


def test_ooo_variations(env):
    benchmark_specs = {"name": env.testName,
                       "args": ["--key-prefix", "", "--distinct-client-seed", "--command-key-pattern=P"]}
    master_nodes_list = env.getMasterNodesList()
    print(",".join(
        ["#clients", "compressed", "chunk_size", "chunkCount", "total_samples", "memoryUsage", "ooo_percentage", "ops_sec", "p50", "p99",
         "p999", "usecs_per_call"]))
    pipeline = 50 
    for n_clients in [1, 2, 3, 4]:
        for compressed in [False, True]:
            for chunk_size in [1, 2, 4, 8, 16, 32, 64, 128, 256, 512, 1024, 2048, 4096, 8192]:
                # Create a temporary directory
                test_dir = tempfile.mkdtemp()
                config = get_default_memtier_config(1, n_clients, 50000, pipeline, "ts.add ts __key__ __key__")
                add_required_env_arguments(benchmark_specs, config, env, master_nodes_list)

                config = RunConfig(test_dir, env.testName, config, {})
                ensure_clean_benchmark_folder(config.results_dir)

                benchmark = Benchmark.from_json(config, benchmark_specs)
                if compressed:
                    env.execute_command('TS.CREATE', 'ts', 'CHUNK_SIZE', chunk_size)
                else:
                    env.execute_command('TS.CREATE', 'ts', 'UNCOMPRESSED', 'CHUNK_SIZE', chunk_size)

                # benchmark.run() returns True if the return code of memtier_benchmark was 0
                memtier_ok = benchmark.run()
                debugPrintMemtierOnError(config, env, memtier_ok)
                p50 = None
                p99 = None
                p999 = None
                with open('{0}/mb.json'.format(config.results_dir)) as jj:
                    results_json = json.load(jj)
                    tsadd_memtier_json = results_json['ALL STATS']['Ts.adds']
                    ops_sec = tsadd_memtier_json['Ops/sec']
                    p_lat = tsadd_memtier_json['Percentile Latencies']
                    p50 = p_lat['p50.00']
                    p99 = p_lat['p99.00']
                    p999 = p_lat['p99.90']

                master_nodes_connections = env.getOSSMasterNodesConnectionList()
                output = env.execute_command('ts.info', 'ts')
                total_samples = float(output[1])
                ooo_percentage = float(output[3])
                memoryUsage = float(output[5])
                chunkCount = float(output[13])

                merged_command_stats = {'cmdstat_ts.add': {'calls': 0, 'usec': 0}}
                overall_request_count = agg_info_commandstats(master_nodes_connections, merged_command_stats)
                ts_add_stats = merged_command_stats['cmdstat_ts.add']
                usecs_per_call = float(ts_add_stats['usec']) / float(ts_add_stats['calls'])
                print(",".join([str(x) for x in
                                [n_clients, compressed, chunk_size, chunkCount, total_samples, memoryUsage, ooo_percentage, ops_sec,
                                 p50, p99, p999, usecs_per_call]]))
                env.execute_command('flushall')
                env.execute_command('config','resetstat')
