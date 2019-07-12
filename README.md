# target-s3-avro

A [Singer](https://singer.io) target that reads in tap input and writes an avro data file and schema to an S3 Bucket using Boto3.

## How to use it

`target-s3-avro` works together with any other [Singer Tap] to move data from sources like [Braintree], [Freshdesk] and [Hubspot] to an s3 bucket in AWS.

### Install

We will use [`tap-exchangeratesapi`][Exchangeratesapi] to pull currency exchange rate data from a public data set as an example.

First, make sure Python 3 is installed on your system or follow these installation instructions for [Mac] or [Ubuntu].

It is recommended to install each Tap and Target in a separate Python virtual environment to avoid conflicting dependencies between any Taps and Targets.

```bash
 # Install tap-exchangeratesapi in its own virtualenv
python3 -m venv ~/.virtualenvs/tap-exchangeratesapi
source ~/.virtualenvs/tap-exchangeratesapi/bin/activate
pip install tap-exchangeratesapi
deactivate

# Install target-s3-avro in its own virtualenv
python3 -m venv ~/.virtualenvs/target-s3-avro
source ~/.virtualenvs/target-s3-avro/bin/activate
pip install <location of cloned target-s3-avro repository>/
deactivate
```

### Run

We can now run `tap-exchangeratesapi` and pipe the output to `target-s3-avro`.

`target-s3-avro` requires a configuration file to set connection parameters like the access keys and target bucket - see [sample_config.json](sample_config.json) for the full field descriton:

```bash
{
  "aws_access_key_id": "<Your AWS Access key>",
  "aws_secret_access_key": "<Your AWS Secret Access key>",
  "target_bucket_key": "<Target S3 Bucket>/<Target S3 Key>",
  "target_schema_bucket_key": "<Target S3 Bucket for schema>/<Target S3 Key for schema>",
  "include_timestamp": "<Set to false to prevent the inclusion of the timestamp in the filenames>",
  "tmp_dir": "Working folder used for creation of temp directory where files will be created before moving to s3"
}
```
* NOTE: The `<Target S3 Key>` portion of the `target_bucket_key` value is treated as a prefix to the key file (see below)
* NOTE: The `<Target S3 Key for schema>` portion of the `target_schema_bucket_key` value is treated as a prefix to the key file (see below)

To run `target-s3-avro` with the configuration file, use this command:

```bash
~/.virtualenvs/tap-exchangeratesapi/bin/tap-exchangeratesapi | ~/.virtualenvs/target-s3-avro/bin/target-s3-avro -c my-config.json
```

The data will be written to a file in the `<Target S3 Bucket>` bucket, with the following key `<Target S3 Key>/exchange_rate-{timestamp}.avro`.
The schema will be written to a file in the `<Target S3 Bucket for schema>` bucket, with the following key `<Target S3 Key for schema>/exchange_rate-{timestamp}.avsc`.

---

Copyright &copy; 2019 Stitch

[Singer Tap]: https://singer.io
[Apache Avro]: https://avro.apache.org/docs/current/
[AWS Boto3]: https://boto3.amazonaws.com/v1/documentation/api/latest/index.html?id=docs_gateway
[Exchangeratesapi]: https://github.com/singer-io/tap-exchangeratesapi
[Mac]: http://docs.python-guide.org/en/latest/starting/install3/osx/
[Ubuntu]: https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04
