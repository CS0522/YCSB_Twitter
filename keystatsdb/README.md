<!--
Copyright (c) 2012 - 2018 YCSB contributors. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You
may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. See accompanying
LICENSE file.
-->

## Quick Start

### 1. Set Up YCSB

Clone the YCSB git repository and compile:

    git clone https://github.com/CS0522/YCSB_Twitter.git
    cd YCSB
    mvn -pl site.ycsb:keystatsdb-binding -am clean package

### 2. Run YCSB

Now you are ready to run! First, load the data:

    ./bin/ycsb load keystatsdb -s -P workloads/workloada

Then, run the workload:

    ./bin/ycsb run keystatsdb -s -P workloads/workloada

