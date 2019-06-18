# target-s3-avro

A [Singer](https://singer.io) target that writes raw tap file data to an S3 Bucket using Boto3.

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
pip install target-s3-avro
deactivate
```

### Run

We can now run `tap-exchangeratesapi` and pipe the output to `target-s3-avro`.

`target-s3-avro` requires a configuration file to set connection parameters like the access keys and target bucket - see [sample_config.json](sample_config.json) for the full field descriton:

```bash
{
  "aws_access_key_id": "<Your AWS Access key>",
  "aws_secret_access_key": "<Your AWS Secret Access key>",
  "target_bucket_key": "<Target S3 Bucket>/<Target S3 Key>"
}
```
* NOTE: The `<Target S3 Key>` portion of the `target_bucket_key` value is treated as a prefix to the key file (see below)

To run `target-s3-avro` with the configuration file, use this command:

```bash
~/.virtualenvs/tap-exchangeratesapi/bin/tap-exchangeratesapi | ~/.virtualenvs/target-s3-avro/bin/target-s3-avro -c my-config.json
```

The data will be written to a file in the `<Target S3 Bucket>` bucket, with the following key `<Target S3 Key>/exchange_rate-{timestamp}.json`.

```bash
› cat exchange_rate-{timestamp}.json
{"ZAR": 14.8707591461, "EUR": 0.885818053, "ILS": 3.5902205687, "MYR": 4.1693684117, "ISK": 125.3432544955, "RUB": 64.6004960581, "BGN": 1.732482948, "SGD": 1.3661971831, "date": "2019-06-13T00:00:00Z", "DKK": 6.615112056, "SEK": 9.475418549,
"PHP": 51.8699619098, "THB": 31.2250863673, "GBP": 0.7879174418, "HKD": 7.8284170431, "JPY": 108.4595624059, "NZD": 1.5236956329, "TRY": 5.8767827088, "RON": 4.1829214279, "CNY": 6.9221365931, "IDR": 14293.0020373815, "CAD": 1.3305872974,
"NOK": 8.6562140136, "USD": 1.0, "MXN": 19.1676853574, "BRL": 3.8470192223, "CZK": 22.6601116131, "CHF": 0.992736292, "KRW": 1183.2226060767, "HUF": 285.233413057, "HRK": 6.5663920631, "AUD": 1.4470723713, "PLN": 3.7712817787, "INR": 69.5141287979}
```

---

Copyright &copy; 2019 Stitch

[Singer Tap]: https://singer.io
[Exchangeratesapi]: https://github.com/singer-io/tap-exchangeratesapi
[Mac]: http://docs.python-guide.org/en/latest/starting/install3/osx/
[Ubuntu]: https://www.digitalocean.com/community/tutorials/how-to-install-python-3-and-set-up-a-local-programming-environment-on-ubuntu-16-04
