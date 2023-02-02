macro_rules! set_list_val {
    // primitive inner
    ($builder:expr,$i:expr,$builder_typ:ty,$value_option:expr,$getter:ident,$value_typ:ty) => {{
        let b: &mut ListBuilder<$builder_typ> = $builder.field_builder($i).unwrap();

        match $value_option {
            Some(lst) => {
                for v in lst {
                    b.values().append_value(v.$getter().unwrap());
                }
                b.append(true);
            }
            None => {
                b.values().append_null();
                b.append(false);
            }
        }
        Ok(())
    }};
}

pub(crate) use set_list_val;
