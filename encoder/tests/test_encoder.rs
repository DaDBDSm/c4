use {encoder::{decode_value, encode_value, Field, Value}};
use std::io::Cursor;

#[test]
fn test_encoder() {
    let inner_message_fields = vec![
        Field {
            number: 1,
            value: Value::Int32(1)
        },
        Field {
            number: 2,
            value: Value::String("hello".to_string())
        }
    ];
    let inner_message = Value::Message(inner_message_fields);
    let value = Value::Message(vec![
        Field {
            number: 1,
            value: Value::Int32(42)
        },
        Field {
            number: 2,
            value: Value::String("hello".to_string())
        },
        Field {
            number: 3,
            value: inner_message
        }
    ]);

    let bytes = encode_value(&value);

    let decoded = decode_value(&mut Cursor::new(&bytes));

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