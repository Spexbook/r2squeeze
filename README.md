# r2squeeze

Compresses all objects in a given R2 bucket using Brotli.

```bash
r2squeeze --bucket <bucket> \
	--account-id <account-id> \
	--key-id <key-id> \
	--secret <secret> \
	--strategy <file|r2> \
	--jobs 5
```
