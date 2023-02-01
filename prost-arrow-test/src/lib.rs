mod protos {
    include!(concat!(env!("OUT_DIR"), "/eto.pb2arrow.tests.rs"));
}

#[cfg(test)]
mod test {
    use super::protos::{MessageWithNestedEnum, SomeRandomEnum};

    #[test]
    fn test_enums() {
        MessageWithNestedEnum {
            status: SomeRandomEnum::Failing.into(),
        };
    }
}
