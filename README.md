# S3 Multipart Adapter (Smua)


## Application Usage

Send a multipart/form-data request with a file upload. Example using curl:

```bash
curl 'http://localhost:5000' \
  --verbose \
  --request POST \
  --header 'Content-Type: multipart/form-data' \
  --form "file=@pyproject.toml;headers=\"Content-Length: $(stat --printf="%s" pyproject.toml)\""
```
