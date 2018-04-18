# HBase Bulk Load Example

This project should be a template for HBase Bulk Load Jobs using MapReduce.

It will _not_ work out of the box. One reason for this is that the business logic in the mapper is not implemented and the Driver doesn't set an InputFormat or any input data.

It is coded against the HBase 1.2.0 API.

*Note*: I have not tested the Secure Bulk Load 

## Template vs. Example

There are two sets of classes in here. One ending in `Template` will _not_ run out of the box. You'll need to customize it to your needs.

The other - ending in `Example` - should run. It treats all input files as text files.
