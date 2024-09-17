# BSVDB Blockarchive Plans

The BSVDB Blockarchive is a system for maintaining archival data concerning the contents of BSV blockchain blocks. It
defines an
API for accessing block data.

A key criteria for this system is that it is designed to be available to everyone at all layers.

* There may be public API's available which can be used directly by clients.
* It is possible to deploy a local instance of the API.
* It is possible to make the local instance available to the public or selected clients.
* An instance can overlay local and remote data sources.

For example: a home user could deploy a instance with limited local storage which fetches new blocks from the Bitcoin
network
and uses remote services for older blocks. Older blocks are purged from the local storage as required to make space for
new blocks.

# Block Data Structures

# Architecture

The blockarchive consists of several layers:

* API - this defines how applications can access the data in the blockarchive
    * Clients - these are known client libraries or applications that can access a blockarchive
* Services - these are software that can be configured and run to maintain a blockarchive and provide API services
* Data Storage - mechanisms to support overlays of local and remote data storage, with retention policies for local data
  and caching capabilities

The BSVDB Blockarchive (will) present several options for each layer, which should fit a variety of needs. The system is
designed to be flexible so that it can be adapted to evolving needs.

## API

### Clients

## Services

## Data Storage

### Comparison of storage technologies

#### File Storage

#### S3

#### IPFS

# Notes

Various bits and pieces, not sure if I'm going to include these in the documentation.

The focus here is
on archive data, it is expected that there will be significant latency between blocks being produced on a live network
and the availability of that data in a remote blockarchive. However, the blockcarchive is also designed so that it can
overlay local copies of data, for example recent blocks, with historical data from remote services.