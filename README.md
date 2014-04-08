OCaml memcached client using [core](https://github.com/janestreet/core), [async](https://github.com/janestreet/async) and [bitstring](https://code.google.com/p/bitstring/).

## Example

```ocaml
open Core.Std
open Async.Std

module Ints = struct
    type t = int
    let to_string = string_of_int
    let of_string = int_of_string
end

module IntCache = Memcached.Make(Ints)

let main () =
	IntCache.connect "localhost" 11212 >>= fun cache ->
	IntCache.set cache "foo" 123 >>= fun result ->
	IntCache.get cache "foo" >>= fun result ->
	IntCache.close cache
in
main ();
Scheduler.go ()
```

## Status

This project is work in progress.

Issues include:

- It hasn't been packaged up for OPAM yet.
- Insufficient handling of 32 bit ints from memcached.
- bitstring is currently a dependency. I'm considering getting rid of it or fix the string copying which is currently happening under the covers (`Bitstring.string_of_bitstring`).
