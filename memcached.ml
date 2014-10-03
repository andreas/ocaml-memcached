open Core.Std
open Async
open Async.Std
open Async_unix

type t = {
  reader: Reader.t;
  writer: Writer.t;
}

exception ProtocolError of string

type opcode =
  | Get
  | Set
  | Add
  | Replace
  | Delete
  | Increment
  | Decrement
  | Quit
  | Flush
  | GetQ
  | NoOp
  | Version
  | GetK
  | GetKQ
  | Append
  | Prepend
  | Stat
  | SetQ
  | AddQ
  | ReplaceQ
  | DeleteQ
  | IncrementQ
  | DecrementQ
  | QuitQ
  | FlushQ
  | AppendQ
  | PrependQ

let opcode_from_int = function
  | 0x00 -> Get
  | 0x01 -> Set
  | 0x02 -> Add
  | 0x03 -> Replace
  | 0x04 -> Delete
  | 0x05 -> Increment
  | 0x06 -> Decrement
  | 0x07 -> Quit
  | 0x08 -> Flush
  | 0x09 -> GetQ
  | 0x0A -> NoOp
  | 0x0B -> Version
  | 0x0C -> GetK
  | 0x0D -> GetKQ
  | 0x0E -> Append
  | 0x0F -> Prepend
  | 0x10 -> Stat
  | 0x11 -> SetQ
  | 0x12 -> AddQ
  | 0x13 -> ReplaceQ
  | 0x14 -> DeleteQ
  | 0x15 -> IncrementQ
  | 0x16 -> DecrementQ
  | 0x17 -> QuitQ
  | 0x18 -> FlushQ
  | 0x19 -> AppendQ
  | 0x1A -> PrependQ
  | i    -> raise (ProtocolError (sprintf "Unknown opcode %d" i))

let opcode_to_int = function
  | Get        -> 0x00
  | Set        -> 0x01
  | Add        -> 0x02
  | Replace    -> 0x03
  | Delete     -> 0x04
  | Increment  -> 0x05
  | Decrement  -> 0x06
  | Quit       -> 0x07
  | Flush      -> 0x08
  | GetQ       -> 0x09
  | NoOp       -> 0x0A
  | Version    -> 0x0B
  | GetK       -> 0x0C
  | GetKQ      -> 0x0D
  | Append     -> 0x0E
  | Prepend    -> 0x0F
  | Stat       -> 0x10
  | SetQ       -> 0x11
  | AddQ       -> 0x12
  | ReplaceQ   -> 0x13
  | DeleteQ    -> 0x14
  | IncrementQ -> 0x15
  | DecrementQ -> 0x16
  | QuitQ      -> 0x17
  | FlushQ     -> 0x18
  | AppendQ    -> 0x19
  | PrependQ   -> 0x1A

type status =
  | NoError
  | KeyNotFound
  | KeyExists
  | ValueTooLarge
  | InvalidArguments
  | ItemNotStored
  | IncrDecrOnNonNumericValue
  | UnknownCommand
  | OutOfMemory

let status_from_int = function
  | 0x0000 -> NoError
  | 0x0001 -> KeyNotFound
  | 0x0002 -> KeyExists
  | 0x0003 -> ValueTooLarge
  | 0x0004 -> InvalidArguments
  | 0x0005 -> ItemNotStored
  | 0x0006 -> IncrDecrOnNonNumericValue
  | 0x0081 -> UnknownCommand
  | 0x0082 -> OutOfMemory
  | i      -> raise (ProtocolError (sprintf "Unknown status %d" i))

let status_to_int = function
  | NoError                   -> 0x0000
  | KeyNotFound               -> 0x0001
  | KeyExists                 -> 0x0002
  | ValueTooLarge             -> 0x0003
  | InvalidArguments          -> 0x0004
  | ItemNotStored             -> 0x0005
  | IncrDecrOnNonNumericValue -> 0x0006
  | UnknownCommand            -> 0x0081
  | OutOfMemory               -> 0x0082

let status_to_string = function
  | NoError                   -> "NoError"
  | KeyNotFound               -> "KeyNotFound"
  | KeyExists                 -> "KeyExists"
  | ValueTooLarge             -> "ValueTooLarge"
  | InvalidArguments          -> "InvalidArguments"
  | ItemNotStored             -> "ItemNotStored"
  | IncrDecrOnNonNumericValue -> "IncrDecrOnNonNumericValue"
  | UnknownCommand            -> "UnknownCommand"
  | OutOfMemory               -> "OutOfMemory"

type data_type =
  | RawBytes

let data_type_from_int = function
  | 0x00 -> RawBytes
  | i    -> raise (ProtocolError (sprintf "Unknown data type %d" i))

let data_type_to_int = function
  | RawBytes -> 0x00

type response = {
  opcode : opcode;
  key_length : int;
  extras_length : int;
  data_type : data_type;
  status : status;
  total_body_length : int;
  opaque : int32;
  cas : int64;
  extras : string;
  key : string;
  value : string;
}

let header_length = 24
let value_length t =
  t.total_body_length - t.key_length - t.extras_length

let parse_response bigstr pos =
  Bigstring.(
    if (unsafe_get_uint8 bigstr 0) <> 0x81 then
      raise (ProtocolError "Invalid magic, expected 0x81")
    else
      let opcode            = unsafe_get_uint8      bigstr (pos + 1)
      and key_length        = unsafe_get_uint16_be  bigstr (pos + 2)
      and extras_length     = unsafe_get_uint8      bigstr (pos + 4)
      and data_type         = unsafe_get_uint8      bigstr (pos + 5)
      and status            = unsafe_get_uint16_be  bigstr (pos + 6)
      and total_body_length = unsafe_get_uint32_be  bigstr (pos + 8)
      and opaque            = unsafe_get_int32_t_be bigstr (pos +12)
      and cas               = unsafe_get_int64_t_be bigstr (pos +16)
      in
      let extras            = get_padded_fixed_string ~padding:' ' bigstr ~pos:(pos +24) ~len:extras_length ()
      and key               = get_padded_fixed_string ~padding:' ' bigstr ~pos:(pos +24 + extras_length) ~len:key_length ()
      and value             = get_padded_fixed_string ~padding:' ' bigstr ~pos:(pos +24 + extras_length + key_length) ~len:(total_body_length - extras_length - key_length) ()
      in {
        opcode = opcode_from_int opcode;
        key_length;
        extras_length;
        data_type = data_type_from_int data_type;
        status = status_from_int status;
        total_body_length;
        opaque;
        cas;
        extras;
        key;
        value;
      }
  )

let string_option_length = Option.value_map ~f:String.length ~default:0

let connect ~host ~port =
  Tcp.to_host_and_port host port
  |> Tcp.connect
  >>| fun (_, reader, writer) ->
    { reader;
      writer;
    }

let length_cmd (data_type, opaque, cas, value, extras, key, opcode) =
  let key_length        = string_option_length key    in
  let extras_length     = string_option_length extras in
  let value_length      = string_option_length value  in
  let total_body_length = key_length + extras_length + value_length in
  header_length + total_body_length

let blit_cmd ~src:(data_type, opaque, cas, value, extras, key, opcode) ~src_pos ~dst ~dst_pos ~len =
  let key_length        = string_option_length key    in
  let extras_length     = string_option_length extras in
  let value_length      = string_option_length value  in
  let total_body_length = key_length + extras_length + value_length in
  Bigstring.(
    unsafe_set_uint8      dst (dst_pos + 0) 0x80;
    unsafe_set_uint8      dst (dst_pos + 1) (opcode_to_int opcode);
    unsafe_set_uint16_be  dst (dst_pos + 2) key_length;
    unsafe_set_uint8      dst (dst_pos + 4) extras_length;
    unsafe_set_uint8      dst (dst_pos + 5) (data_type_to_int data_type);
    unsafe_set_uint16_be  dst (dst_pos + 6) 0;
    unsafe_set_uint32_be  dst (dst_pos + 8) total_body_length;
    unsafe_set_int32_t_be dst (dst_pos +12) opaque;
    unsafe_set_int64_t_be dst (dst_pos +16) cas;
    Option.iter extras (fun extras ->
      set_padded_fixed_string ' ' dst (dst_pos +24) extras_length extras
    );
    Option.iter key (fun key ->
      set_padded_fixed_string ' ' dst (dst_pos +24+extras_length) key_length key
    );
    Option.iter value (fun value ->
      set_padded_fixed_string ' ' dst (dst_pos +24+extras_length+key_length) value_length value
    );
  )

let write_command writer cmd =
  Writer.write_gen ~length:length_cmd ~blit_to_bigstring:blit_cmd writer cmd

let total_body_length_from_bigstr bigstr pos = Bigstring.unsafe_get_int32_be bigstr (pos + 8)

let handle_chunks bigstr ~pos ~len =
  if len >= header_length && len >= header_length + total_body_length_from_bigstr bigstr pos then
    let response = parse_response bigstr pos in
    return (`Stop_consumed (response, header_length + response.total_body_length))
  else
    return `Continue

let response_from_reader reader =
  Reader.read_one_chunk_at_a_time reader handle_chunks >>| function
    | `Eof -> Or_error.error_string "EOF"
    | `Eof_with_unconsumed_data data -> Or_error.error_string (sprintf "EOF with %s" data)
    | `Stopped response -> Ok response

let command ?(data_type=RawBytes) ?(opaque=0l) ?(cas=0L)
?(value=None) ?(extras=None) ?(key=None) t ~opcode =
  try_with (fun () ->
    Monitor.detach_and_iter_errors (Writer.monitor t.writer) ~f:raise;
    write_command t.writer (data_type, opaque, cas, value, extras, key, opcode);
    response_from_reader t.reader
  ) >>| function
      | Ok response -> response
      | Error exn   -> Or_error.of_exn exn


let set_add_replace opcode ?(flags=0l) ?(expiry=0l) t ~key ~value =
  let extras = String.create 8 in
  EndianString.BigEndian.set_int32 extras 0 flags;
  EndianString.BigEndian.set_int32 extras 4 expiry;
  command t ~opcode:opcode ~key:(Some key) ~value:(Some value) ~extras:(Some extras) >>|? fun response ->
  response.status = NoError

let set     = set_add_replace Set
let add     = set_add_replace Add
let replace = set_add_replace Replace

let delete t ~key =
  command t ~opcode:Delete ~key:(Some key) >>|? fun response ->
  response.status = NoError

let get t ~key =
  command t ~opcode:Get ~key:(Some key) >>=? fun response ->
  if response.status = NoError then
    return (Ok response.value)
  else
    return (Or_error.error_string "Not found")

let arith t opcode key initial delta expiry =
  let extras = String.create 20 in
  EndianString.BigEndian.set_int64 extras  0 delta;
  EndianString.BigEndian.set_int64 extras  8 initial;
  EndianString.BigEndian.set_int32 extras 16 expiry;
  command t ~opcode ~key:(Some key) ~extras:(Some extras) >>=? fun response ->
  if response.status = NoError then
    return (Ok (EndianString.BigEndian.get_int64 response.value 0))
  else
    return (Or_error.error_string (sprintf "Memcached status %s\n" (status_to_string response.status)))

let incr ?(expiry=0l) t ~key ~initial ~delta =
  arith t Increment key initial delta expiry

let decr ?(expiry=0l) t ~key ~initial ~delta =
  arith t Decrement key initial delta expiry

let prepend t ~key ~value =
  command t ~opcode:Prepend ~key:(Some key) ~value:(Some value) >>|? fun response ->
  response.status = NoError

let append t ~key ~value =
  command t ~opcode:Append ~key:(Some key) ~value:(Some value) >>|? fun response ->
  response.status = NoError

let flush ?(expiry=0l) t =
  let extras = String.create 4 in
  EndianString.BigEndian.set_int32 extras 0 expiry;
  command t ~opcode:Flush ~extras:(Some extras) >>|? fun response ->
  response.status = NoError

let close t =
  Writer.close t.writer >>= fun () ->
  Reader.close t.reader
