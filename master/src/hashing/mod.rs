pub mod consistent;

pub fn get_key_for_object(bucket_name: &str, object_key: &str) -> String {
    format!("{}_{}", bucket_name, object_key)
}
