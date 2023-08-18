---
title: "Generic Codecs"
linkTitle: "Generic Codecs"
type: docs
description: >
  How to write codecs that make your life easier
---

Codecs convert between `[]byte` and types used in the application. Only in rare cases, new marshalling/encoding mechanisms will be required.
Most of the time, marshalling via existin techniques like `json` or `proto` is sufficient. Since those packages are type-independent, it does make sense to create codecs using generics.

{{< gist frairon c8d650ff29db60fc7d42bf64bfab277c "json_codec.go" >}}
