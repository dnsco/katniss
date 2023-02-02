macro_rules! set_list_val {
    // primitive inner
    ($b:expr,$value_option:expr,$getter:ident,$value_typ:ty) => {{
        let b = $b;
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
