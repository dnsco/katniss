/// append a value to given builder
macro_rules! set_value {
    ($builder:expr,$i:expr,$typ:ty,$getter:ident,$value:expr) => {{
        let b: &mut $typ = $builder.field_builder($i).unwrap();
        match $value {
            Some(val) => {
                let v = val.$getter().ok_or_else(|| {
                    let msg = format!("Could not cast {} to correct type", val);
                    Error::new(InvalidData, msg)
                })?;
                b.append_value(v)
            }
            None => b.append_null(),
        }
    }};
}

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
pub(crate) use set_value;
