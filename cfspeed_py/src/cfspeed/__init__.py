"""cfspeed — Cloudflare speed test library.

Usage:
    import cfspeed

    client = cfspeed.Client(parallel_streams=8)
    result = client.run(timeout=30)
    print(result)
    print(result.json())
"""

from cfspeed.client import Client, Options, Result, parse_size, CfspeedError

__version__ = "1.0.0"
__all__ = ["Client", "Options", "Result", "parse_size", "CfspeedError"]
