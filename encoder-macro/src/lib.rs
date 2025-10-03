use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput, Field, LitInt, Type, parenthesized};

#[proc_macro_derive(C4Encode, attributes(c4_encode))]
pub fn c4encode_macro_derive(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;

    let fields = match input.data {
        syn::Data::Struct(s) => s.fields,
        _ => panic!("C4Encode can only be derived for structs"),
    };

    // Collect per-field metadata needed to generate code
    let mut serialize_field_chunks = Vec::new();
    let mut match_arms = Vec::new();
    let mut init_stmts = Vec::new();
    let mut build_fields = Vec::new();

    for field in fields.into_iter() {
        let ident = field.ident.clone().expect("Unnamed fields are not supported");
        let number = parse_field_number(&field)
            .expect("failed to parse c4_encode(number = N)")
            .expect("missing c4_encode(number = N) attribute");

        match parse_field_type(&field) {
            Ok(FieldKind::Int32) => {
                serialize_field_chunks.push(quote! {
                    {
                        let mut __fb = Vec::new();
                        __fb.extend_from_slice(&(#number as u32).to_be_bytes());
                        __fb.extend_from_slice(&::encoder::encode_value(&::encoder::Value::Int32(self.#ident)));
                        __fb
                    }
                });
                match_arms.push(quote! {
                    #number => { if let ::encoder::Value::Int32(v) = field.value { #ident = v; } }
                });
                init_stmts.push(quote! { let mut #ident: i32 = 0; });
                build_fields.push(quote! { #ident: #ident });
            }
            Ok(FieldKind::String) => {
                serialize_field_chunks.push(quote! {
                    {
                        let mut __fb = Vec::new();
                        __fb.extend_from_slice(&(#number as u32).to_be_bytes());
                        __fb.extend_from_slice(&::encoder::encode_value(&::encoder::Value::String(self.#ident.clone())));
                        __fb
                    }
                });
                match_arms.push(quote! {
                    #number => { if let ::encoder::Value::String(v) = field.value { #ident = v; } }
                });
                init_stmts.push(quote! { let mut #ident: ::std::string::String = ::std::string::String::new(); });
                build_fields.push(quote! { #ident: #ident });
            }
            Ok(FieldKind::Message(inner_ty)) => {
                serialize_field_chunks.push(quote! {
                    {
                        let mut __fb = Vec::new();
                        __fb.extend_from_slice(&(#number as u32).to_be_bytes());
                        let nested_bytes = <#inner_ty as ::encoder::C4Encode>::serialize(&self.#ident);
                        __fb.extend_from_slice(nested_bytes.as_slice());
                        __fb
                    }
                });
                match_arms.push(quote! {
                    #number => {
                        if let ::encoder::Value::Message(nested_fields) = field.value {
                            let nested_bytes = {
                                let mut __b = Vec::new();
                                __b.extend_from_slice(&3u32.to_be_bytes());
                                __b.extend_from_slice(&(nested_fields.len() as u32).to_be_bytes());
                                for nf in nested_fields {
                                    __b.extend_from_slice(&nf.number.to_be_bytes());
                                    match nf.value {
                                        ::encoder::Value::Message(inner) => {
                                            // recursive manual encoding
                                            let inner_bytes = {
                                                let mut __ib = Vec::new();
                                                __ib.extend_from_slice(&3u32.to_be_bytes());
                                                __ib.extend_from_slice(&(inner.len() as u32).to_be_bytes());
                                                for inf in inner {
                                                    __ib.extend_from_slice(&inf.number.to_be_bytes());
                                                    __ib.extend_from_slice(&::encoder::encode_value(&inf.value));
                                                }
                                                __ib
                                            };
                                            __b.extend_from_slice(inner_bytes.as_slice());
                                        }
                                        ref other => {
                                            __b.extend_from_slice(&::encoder::encode_value(other));
                                        }
                                    }
                                }
                                __b
                            };
                            #ident = <#inner_ty as ::encoder::C4Encode>::deserialize(&nested_bytes);
                        }
                    }
                });
                init_stmts.push(quote! {
                    let mut #ident: #inner_ty = {
                        let mut __b = Vec::new();
                        __b.extend_from_slice(&3u32.to_be_bytes());
                        __b.extend_from_slice(&0u32.to_be_bytes());
                        <#inner_ty as ::encoder::C4Encode>::deserialize(__b.as_slice())
                    };
                });
                build_fields.push(quote! { #ident: #ident });
            }
            Err(e) => panic!("{}", e),
        }
    }

    let expanded = quote! {
        impl ::encoder::C4Encode for #name {
            fn serialize(&self) -> Vec<u8> {
                let mut field_chunks: Vec<Vec<u8>> = Vec::new();
                #(
                    field_chunks.push({ #serialize_field_chunks });
                )*
                let mut out = Vec::new();
                out.extend_from_slice(&3u32.to_be_bytes());
                out.extend_from_slice(&((field_chunks.len() as u32)).to_be_bytes());
                for chunk in field_chunks { out.extend_from_slice(&chunk); }
                out
            }

            fn deserialize(bytes: &[u8]) -> Self {
                let mut cursor = ::std::io::Cursor::new(bytes);
                let value = ::encoder::decode_value(&mut cursor);
                #(#init_stmts)*
                if let ::encoder::Value::Message(fields) = value {
                    for field in fields {
                        match field.number {
                            #(#match_arms,)*
                            _ => {}
                        }
                    }
                }
                #name { #(#build_fields,)* }
            }
        }
    };

    TokenStream::from(expanded)
}

#[derive(Clone)]
enum FieldKind { Int32, String, Message(Type) }

fn parse_field_number(ast_field: &Field) -> syn::Result<Option<u32>> {
    let mut field_number: Option<u32> = None;
    for attr in &ast_field.attrs {
        if attr.path().is_ident("c4_encode") {
            // Accept either `#[c4_encode(number = N)]` or `#[c4_encode(num(N))]`
            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("number") {
                    meta.input.parse::<syn::Token![=]>()?;
                    let lit: LitInt = meta.input.parse()?;
                    let num = lit.base10_parse()?;
                    if field_number.is_some() {
                        return Err(syn::Error::new_spanned(&meta.path, "duplicate 'number' attribute found"));
                    }
                    field_number = Some(num);
                } else if meta.path.is_ident("num") {
                    let content;
                    parenthesized!(content in meta.input);
                    let lit: LitInt = content.parse()?;
                    let num = lit.base10_parse()?;
                    if field_number.is_some() {
                        return Err(syn::Error::new_spanned(&meta.path, "duplicate 'num' attribute found"));
                    }
                    field_number = Some(num);
                }
                Ok(())
            })?;
        }
    }
    Ok(field_number)
}

fn parse_field_type(ast_field: &Field) -> syn::Result<FieldKind> {
    match &ast_field.ty {
        syn::Type::Path(p) if p.path.is_ident("i32") => Ok(FieldKind::Int32),
        syn::Type::Path(p) if p.path.is_ident("String") => Ok(FieldKind::String),
        syn::Type::Path(_) => Ok(FieldKind::Message(ast_field.ty.clone())),
        other => Err(syn::Error::new_spanned(other, "invalid field type")),
    }
}