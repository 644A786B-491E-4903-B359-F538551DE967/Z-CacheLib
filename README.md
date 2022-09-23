# Z-CacheLib

Project is cloned from <https://github.com/facebook/CacheLib>

## Building and installation

1. You should install libzbd and libexplain at first.
2. Follow the steps provided by [CacheLib](https://cachelib.org/docs/installation/installation/)

## Run CacheBench

You should change device path in config file appropiately before you run the CacheBench.

```bash
./opt/cachelib/bin/cachebench --json_test_config ./configs/kvcache_l2_wc/config.json --progress_stats_file=/tmp/mc-l2-reg.log --logging=INFO
```

More configuration files can be found in `./configs/`. If you want to run evaluations for ZNS-Middle, you should change project to branch `middle` rather than the default `main` branch.
