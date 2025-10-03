use encoder::{Field, Value, decode_value, encode_value, EncodeError};
use std::io::Cursor;

#[test]
fn test_encoder() {
    let inner_message_fields = vec![
        Field {
            number: 1,
            value: Value::Int32(1),
        },
        Field {
            number: 2,
            value: Value::String("hello2".to_string()),
        },
    ];
    let inner_message = Value::Message(inner_message_fields);
    let value = Value::Message(vec![
        Field {
            number: 1,
            value: Value::Int32(42),
        },
        Field {
            number: 2,
            value: Value::String("hello".to_string()),
        },
        Field {
            number: 3,
            value: inner_message,
        },
    ]);

    let bytes = encode_value(&value).unwrap();
    let decoded = decode_value(&mut Cursor::new(&bytes)).unwrap();

    assert_eq!(decoded.type_id(), Value::MESSAGE_ID);
    let Value::Message(fields) = decoded else {
        panic!("Invalid message type")
    };
    assert_eq!(fields.len(), 3);
    assert_eq!(fields.get(0).unwrap().number, 1);
    assert_eq!(fields.get(0).unwrap().value.type_id(), Value::INT32_ID);
    let Value::Int32(i) = fields.get(0).unwrap().value else {
        panic!("Invalid field")
    };
    assert_eq!(i, 42);
}

#[test]
fn test_empty_list() {
    let value = Value::List(vec![]);
    let bytes = encode_value(&value).unwrap();
    let decoded = decode_value(&mut Cursor::new(&bytes)).unwrap();

    assert_eq!(decoded, value);
}

#[test]
fn test_list_of_ints() {
    let value = Value::List(vec![
        Value::Int32(10),
        Value::Int32(20),
        Value::Int32(30),
    ]);
    let bytes = encode_value(&value).unwrap();
    let decoded = decode_value(&mut Cursor::new(&bytes)).unwrap();

    assert_eq!(decoded, value);
}

#[test]
fn test_list_of_strings() {
    let value = Value::List(vec![
        Value::String("foo".to_string()),
        Value::String("bar".to_string()),
    ]);
    let bytes = encode_value(&value).unwrap();
    let decoded = decode_value(&mut Cursor::new(&bytes)).unwrap();

    assert_eq!(decoded, value);
}

#[test]
fn test_heterogeneous_list_rejected() {
    let value = Value::List(vec![
        Value::Int32(1),
        Value::String("oops".to_string()),
    ]);
    let result = encode_value(&value);
    assert!(matches!(result, Err(EncodeError::ListTypeMismatch)));
}
