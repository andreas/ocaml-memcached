open Core.Std
open Async.Std

let test_suite = Test_async.(
  create ~before_all:(fun () ->
           Memcached.connect "localhost" 11212
         )
         ~before:(fun cache ->
           return cache
         )
         ~after:(fun cache ->
           Memcached.flush cache >>= fun _ ->
           Deferred.unit
         )
         ~after_all:(fun cache ->
           Memcached.close cache
         )

  |> test "set" (fun cache ->
    Bool.assert_equal (Memcached.set cache "foo" "1") true
  )

  |> test "get" (fun cache ->
    Memcached.set cache "foo" "1" >>= fun _ ->
    String.assert_equal (Memcached.get cache "foo") "1"
  )

  |> test "get missing" (fun cache ->
    assert_error (Memcached.get cache "foo") "Not found"
  )

  |> test "flush" (fun cache ->
    Memcached.set cache "foo" "1" >>= fun _ ->
    Memcached.flush cache         >>= fun _ ->
    assert_error (Memcached.get cache "foo") "Not found"
  )

  |> test "delete" (fun cache ->
    Memcached.set cache "foo" "1" >>= fun _ ->
    Memcached.delete cache "foo"  >>= fun _ ->
    assert_error (Memcached.get cache "foo") "Not found"
  )

  |> test "incr initial" (fun cache ->
    Int64.assert_equal (Memcached.incr cache ~key:"foo" ~initial:0L ~delta:1L) 0L
  )

  |> test "incr" (fun cache ->
    Int64.assert_equal (Memcached.incr cache ~key:"foo" ~initial:0L ~delta:1L) 0L
    >>= fun _ ->
    Int64.assert_equal (Memcached.incr cache ~key:"foo" ~initial:0L ~delta:1L) 1L
  )

  |> test "set then incr" (fun cache ->
    Bool.assert_equal (Memcached.set cache "foo" "1") true >>= fun _ ->
    Int64.assert_equal (Memcached.incr cache ~key:"foo" ~initial:0L ~delta:2L) 3L
  )

  |> test "decr initial" (fun cache ->
    Int64.assert_equal (Memcached.decr cache ~key:"foo" ~initial:2L ~delta:1L) 2L
  )

  |> test "decr" (fun cache ->
    Int64.assert_equal (Memcached.decr cache ~key:"foo" ~initial:2L ~delta:1L) 2L 
    >>= fun _ ->
    Int64.assert_equal (Memcached.decr cache ~key:"foo" ~initial:2L ~delta:1L) 1L
  )

  |> test "set then decr" (fun cache ->
    Bool.assert_equal (Memcached.set cache "foo" "5") true
    >>= fun _ ->
    Int64.assert_equal (Memcached.decr cache ~key:"foo" ~initial:0L ~delta:2L) 3L
  )

  |> test "add" (fun cache ->
    Bool.assert_equal (Memcached.add cache "foo" "3") true
  )

  |> test "add existing" (fun cache ->
    Memcached.set cache "foo" "3" >>= fun _ ->
    Bool.assert_equal (Memcached.add cache "foo" "3") false
  )

  |> test "replace" (fun cache ->
    Memcached.set cache "foo" "3" >>= fun _ ->
    Bool.assert_equal (Memcached.replace cache "foo" "3") true
  )

  |> test "replace missing" (fun cache ->
    Bool.assert_equal (Memcached.replace cache "foo" "3") false
  )

  |> test "prepend missing" (fun cache ->
    Bool.assert_equal (Memcached.prepend cache "foo" "1") false
    >>= fun _ ->
    assert_error (Memcached.get cache "foo") "Not found"
  )

  |> test "prepend present" (fun cache ->
    Memcached.set cache "foo" "1" >>= fun _ ->
    Memcached.prepend cache "foo" "2" >>= fun _ ->
    String.assert_equal (Memcached.get cache "foo") "21"
  )

  |> test "append missing" (fun cache ->
    Bool.assert_equal (Memcached.append cache "foo" "1") false
    >>= fun _ ->
    assert_error (Memcached.get cache "foo") "Not found"
  )

  |> test "append present" (fun cache ->
    Memcached.set cache "foo" "1" >>= fun _ ->
    Memcached.append cache "foo" "2" >>= fun _ ->
    String.assert_equal (Memcached.get cache "foo") "12"
  )
);;

let main () =
  Test_async.run test_suite >>> fun _ ->
  Async_unix.Shutdown.shutdown 1
in
main ();
Scheduler.go ()
