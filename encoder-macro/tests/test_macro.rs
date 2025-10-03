use encoder_macro::C4Encode;
use encoder::C4Encode;

#[test]
fn test_encoder_macro() {
    let inner = MyInnerStruct { field1: 42 };
    let my = MyStruct { field1: 7, field2: "hi".to_string(), field3: inner };

    let bytes = my.serialize();
    let decoded = MyStruct::deserialize(&bytes);

    assert_eq!(decoded.field1, 7);
    assert_eq!(decoded.field2, "hi");
    assert_eq!(decoded.field3.field1, 42);
}

#[derive(C4Encode)]
struct MyStruct {
    #[c4_encode(number = 1)]
    field1: i32,

    #[c4_encode(number = 2)]
    field2: String,

    #[c4_encode(number = 3)]
    field3: MyInnerStruct,
}

#[derive(C4Encode)]
struct MyInnerStruct {
    #[c4_encode(number = 1)]
    field1: i32,
}