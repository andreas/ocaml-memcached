open Core.Std
open Async.Std

module type S = sig
  type value

  val get :
    Memcached.t ->
    key:string ->
    value Deferred.Or_error.t

  val set :
    ?flags:Int32.t ->
    ?expiry:Int32.t ->
    Memcached.t ->
    key:string ->
    value:value ->
    bool Deferred.Or_error.t

  val add :
    ?flags:Int32.t ->
    ?expiry:Int32.t ->
    Memcached.t ->
    key:string ->
    value:value ->
    bool Deferred.Or_error.t

  val replace :
    ?flags:Int32.t ->
    ?expiry:Int32.t ->
    Memcached.t ->
    key:string ->
    value:value ->
    bool Deferred.Or_error.t
end

module type Value = sig
  type t
  val to_string: t -> string
  val of_string: string -> t
end

module Make (Value : Value) : S with type value := Value.t
