open Core.Std
open Async.Std

module Ints = struct
    type t = int
    let to_string = string_of_int
    let of_string = int_of_string
end

module IntCache = Memcached.Make(Ints)

let int_suite = Test_async.(
  create ~before_all:(fun () ->
           IntCache.connect "localhost" 11212
         )
         ~before:(fun cache ->
           return cache
         )
         ~after:(fun cache ->
           IntCache.flush cache >>= fun _ ->
           Deferred.unit
         )
         ~after_all:(fun cache ->
           IntCache.close cache
         )

  |> test "set" (fun cache ->
    Bool.assert_equal (IntCache.set cache "foo" 1) true
  )

  |> test "get" (fun cache ->
    IntCache.set cache "foo" 1 >>= fun _ ->
    Int.assert_equal (IntCache.get cache "foo") 1
  )

  |> test "get missing" (fun cache ->
    assert_error (IntCache.get cache "foo") "Not found"
  )

  |> test "flush" (fun cache ->
    IntCache.set cache "foo" 1 >>= fun _ ->
    IntCache.flush cache       >>= fun _ ->
    assert_error (IntCache.get cache "foo") "Not found"
  )

  |> test "delete" (fun cache ->
    IntCache.set cache "foo" 1  >>= fun _ ->
    IntCache.delete cache "foo" >>= fun _ ->
    assert_error (IntCache.get cache "foo") "Not found"
  )

  |> test "incr initial" (fun cache ->
    Int64.assert_equal (IntCache.incr cache ~key:"foo" ~initial:0L ~delta:1L) 0L
  )

  |> test "incr" (fun cache ->
    Int64.assert_equal (IntCache.incr cache ~key:"foo" ~initial:0L ~delta:1L) 0L
    >>= fun _ ->
    Int64.assert_equal (IntCache.incr cache ~key:"foo" ~initial:0L ~delta:1L) 1L
  )

  |> test "set then incr" (fun cache ->
    Bool.assert_equal (IntCache.set cache "foo" 1) true >>= fun _ ->
    Int64.assert_equal (IntCache.incr cache ~key:"foo" ~initial:0L ~delta:2L) 3L
  )

  |> test "decr initial" (fun cache ->
    Int64.assert_equal (IntCache.decr cache ~key:"foo" ~initial:2L ~delta:1L) 2L
  )

  |> test "decr" (fun cache ->
    Int64.assert_equal (IntCache.decr cache ~key:"foo" ~initial:2L ~delta:1L) 2L 
    >>= fun _ ->
    Int64.assert_equal (IntCache.decr cache ~key:"foo" ~initial:2L ~delta:1L) 1L
  )

  |> test "set then decr" (fun cache ->
    Bool.assert_equal (IntCache.set cache "foo" 5) true
    >>= fun _ ->
    Int64.assert_equal (IntCache.decr cache ~key:"foo" ~initial:0L ~delta:2L) 3L
  )

  |> test "add" (fun cache ->
    Bool.assert_equal (IntCache.add cache "foo" 3) true
  )

  |> test "add existing" (fun cache ->
    IntCache.set cache "foo" 3 >>= fun _ ->
    Bool.assert_equal (IntCache.add cache "foo" 3) false
  )

  |> test "replace" (fun cache ->
    IntCache.set cache "foo" 3 >>= fun _ ->
    Bool.assert_equal (IntCache.replace cache "foo" 3) true
  )

  |> test "replace missing" (fun cache ->
    Bool.assert_equal (IntCache.replace cache "foo" 3) false
  )

  |> test "prepend missing" (fun cache ->
    Bool.assert_equal (IntCache.prepend cache "foo" 1) false
    >>= fun _ ->
    assert_error (IntCache.get cache "foo") "Not found"
  )

  |> test "prepend present" (fun cache ->
    IntCache.set cache "foo" 1 >>= fun _ ->
    IntCache.prepend cache "foo" 2 >>= fun _ ->
    Int.assert_equal (IntCache.get cache "foo") 21
  )

  |> test "append missing" (fun cache ->
    Bool.assert_equal (IntCache.append cache "foo" 1) false
    >>= fun _ ->
    assert_error (IntCache.get cache "foo") "Not found"
  )

  |> test "append present" (fun cache ->
    IntCache.set cache "foo" 1 >>= fun _ ->
    IntCache.append cache "foo" 2 >>= fun _ ->
    Int.assert_equal (IntCache.get cache "foo") 12
  )
)

module Strings = struct
    type t = string
    let to_string = ident
    let of_string = ident
end

module StringCache = Memcached.Make(Strings)

let string_suite = Test_async.(
  create ~before_all:(fun () ->
           StringCache.connect "localhost" 11212
         )
         ~before:(fun cache ->
           return cache
         )
         ~after:(fun cache ->
           StringCache.flush cache
           >>= fun _ -> Deferred.unit
         )
         ~after_all:(fun cache ->
           StringCache.close cache
         )

  |> test "set" (fun cache ->
    Bool.assert_equal (StringCache.set cache "foo" "bar") true
  )

  |> test "set then get" (fun cache ->
    StringCache.set cache "foo" "bar" >>= fun _ ->
    String.assert_equal (StringCache.get cache "foo") "bar"
  )

  |> test "prepend missing" (fun cache ->
    Bool.assert_equal (StringCache.prepend cache "foo" "bar") false
    >>= fun _ ->
    assert_error (StringCache.get cache "foo") "Not found"
  )

  |> test "prepend present" (fun cache ->
    StringCache.set cache "foo" "bar" >>= fun _ ->
    StringCache.prepend cache "foo" "baz" >>= fun _ ->
    String.assert_equal (StringCache.get cache "foo") "bazbar"
  )

  |> test "append missing" (fun cache ->
    Bool.assert_equal (StringCache.append cache "foo" "bar") false
    >>= fun _ ->
    assert_error (StringCache.get cache "foo") "Not found"
  )

  |> test "append present" (fun cache ->
    StringCache.set cache "foo" "bar" >>= fun _ ->
    StringCache.append cache "foo" "baz" >>= fun _ ->
    String.assert_equal (StringCache.get cache "foo") "barbaz"
  )
)

let main () =
  Test_async.run int_suite >>> fun _ ->
  Test_async.run string_suite >>> fun _ ->
  Async_unix.Shutdown.shutdown 1
in
main ();
Scheduler.go ()
