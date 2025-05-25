use crate::indent_scope;
use crate::util::{is_reducer_invokable, iter_reducers, iter_tables, iter_types, iter_unique_cols};

use super::util::{collect_case, print_auto_generated_file_comment, type_ref_name};

use std::collections::BTreeSet;
use std::fmt::{self, Write};
use std::ops::Deref;

use convert_case::{Case, Casing};
use spacetimedb_lib::sats::AlgebraicTypeRef;
use spacetimedb_schema::def::{ModuleDef, ReducerDef, ScopedTypeName, TableDef, TypeDef};
use spacetimedb_schema::identifier::Identifier;
use spacetimedb_schema::schema::{Schema, TableSchema};
use spacetimedb_schema::type_for_generate::{AlgebraicTypeDef, AlgebraicTypeUse, PrimitiveType};

use super::code_indenter::{CodeIndenter, Indenter};
use super::Lang;
use spacetimedb_lib::version::spacetimedb_lib_version;

type Imports = BTreeSet<AlgebraicTypeRef>;

const INDENT: &str = "    ";

pub struct Python;

impl Lang for Python {
    fn table_filename(
        &self,
        _module: &spacetimedb_schema::def::ModuleDef,
        table: &spacetimedb_schema::def::TableDef,
    ) -> String {
        table_module_name(&table.name) + ".py"
    }

    fn type_filename(&self, type_name: &ScopedTypeName) -> String {
        type_module_name(type_name) + ".py"
    }

    fn reducer_filename(&self, reducer_name: &Identifier) -> String {
        reducer_module_name(reducer_name) + ".py"
    }

    fn generate_type(&self, module: &ModuleDef, typ: &TypeDef) -> String {
        let type_name = collect_case(Case::Pascal, typ.name.name_segments());

        let mut output = CodeIndenter::new(String::new(), INDENT);
        let out = &mut output;

        print_file_header(out);

        match &module.typespace_for_generate()[typ.ty] {
            AlgebraicTypeDef::Product(product) => {
                gen_and_print_imports(module, out, &product.elements, &[typ.ty]);
                define_product_type(module, out, &type_name, &product.elements);
            }
            AlgebraicTypeDef::Sum(sum) => {
                gen_and_print_imports(module, out, &sum.variants, &[typ.ty]);
                define_sum_type(module, out, &type_name, &sum.variants);
            }
            AlgebraicTypeDef::PlainEnum(plain_enum) => {
                let variants = plain_enum
                    .variants
                    .iter()
                    .cloned()
                    .map(|var| (var, AlgebraicTypeUse::Unit))
                    .collect::<Vec<_>>();
                define_enum_type(module, out, &type_name, &variants);
            }
        }
        out.newline();

        output.into_inner()
    }

    fn generate_table(&self, module: &ModuleDef, table: &TableDef) -> String {
        let schema = TableSchema::from_module_def(module, table, (), 0.into())
            .validated()
            .expect("Failed to generate table due to validation errors");

        let mut output = CodeIndenter::new(String::new(), INDENT);
        let out = &mut output;

        print_file_header(out);

        let type_ref = table.product_type_ref;
        let row_type = type_ref_name(module, type_ref);
        let row_type_module = type_ref_module_name(module, type_ref);

        writeln!(out, "from .{row_type_module} import {row_type}");

        let product_def = module.typespace_for_generate()[type_ref].as_product().unwrap();

        // Import the types of all fields.
        gen_and_print_imports(
            module,
            out,
            &product_def.elements,
            &[], // No need to skip any imports; we're not defining a type, so there's no chance of circular imports.
        );

        writeln!(
            out,
            "from typing import List, Optional, Callable, Any"
        );
        writeln!(
            out,
            "from spacetimedb_sdk.spacetimedb_client import SpacetimeDBClient, DbEvent, ReducerEvent"
        );

        let table_name = table.name.deref();
        let table_name_pascalcase = table.name.deref().to_case(Case::Pascal);
        let table_class = table_name_pascalcase.clone() + "Table";

        writeln!(out);

        write!(
            out,
            "class {table_class}:
    \"\"\"
    Table handle for the table `{table_name}`.
    
    Obtain a handle from the SpacetimeDBClient instance.
    \"\"\"
    
    def __init__(self, client: SpacetimeDBClient):
        self.client = client
        self.table_name = \"{table_name}\"
    
    def count(self) -> int:
        \"\"\"Get the number of rows in the table.\"\"\"
        return len(self.client._get_table_cache(self.table_name).get_all())
    
    def iter(self) -> List[{row_type}]:
        \"\"\"Get all rows in the table.\"\"\"
        return list(self.client._get_table_cache(self.table_name).get_all().values())
"
        );

        // Generate unique constraint methods
        for (unique_field_ident, unique_field_type_use) in
            iter_unique_cols(module.typespace_for_generate(), &schema, product_def)
        {
            let unique_field_name = unique_field_ident.deref().to_case(Case::Snake);
            let unique_field_type = type_name(module, unique_field_type_use);

            writeln!(
                out,
                "    
    def find_by_{unique_field_name}(self, {unique_field_name}: {unique_field_type}) -> Optional[{row_type}]:
        \"\"\"Find a row by its {unique_field_name} field.\"\"\"
        for row in self.iter():
            if row.{unique_field_name} == {unique_field_name}:
                return row
        return None"
            );
        }

        writeln!(
            out,
            "
    def on_insert(self, callback: Callable[[str, {row_type}, ReducerEvent], None]) -> None:
        \"\"\"Register a callback for row insertions.\"\"\"
        self.client._register_row_update(self.table_name, 
            lambda op, old_row, new_row, reducer_event: 
                callback(op, new_row, reducer_event) if op == \"insert\" else None)
    
    def on_delete(self, callback: Callable[[str, {row_type}, ReducerEvent], None]) -> None:
        \"\"\"Register a callback for row deletions.\"\"\"
        self.client._register_row_update(self.table_name,
            lambda op, old_row, new_row, reducer_event:
                callback(op, old_row, reducer_event) if op == \"delete\" else None)
"
        );

        if schema.pk().is_some() {
            writeln!(
                out,
                "
    def on_update(self, callback: Callable[[str, {row_type}, {row_type}, ReducerEvent], None]) -> None:
        \"\"\"Register a callback for row updates. Only available for tables with primary keys.\"\"\"
        self.client._register_row_update(self.table_name,
            lambda op, old_row, new_row, reducer_event:
                callback(op, old_row, new_row, reducer_event) if op == \"update\" else None)
"
            );
        }

        output.into_inner()
    }

    fn generate_reducer(&self, module: &ModuleDef, reducer: &ReducerDef) -> String {
        let mut output = CodeIndenter::new(String::new(), INDENT);
        let out = &mut output;

        print_file_header(out);

        out.newline();

        gen_and_print_imports(
            module,
            out,
            &reducer.params_for_generate.elements,
            // No need to skip any imports; we're not emitting a type that other modules can import.
            &[],
        );

        writeln!(out, "from typing import NamedTuple");
        writeln!(out, "from spacetimedb_sdk.spacetimedb_client import SpacetimeDBClient");

        let args_type = reducer_args_type_name(&reducer.name);

        define_product_type(module, out, &args_type, &reducer.params_for_generate.elements);

        let reducer_name = &reducer.name;
        let reducer_function_name = reducer_function_name(reducer);

        if is_reducer_invokable(reducer) {
            writeln!(out);
            writeln!(out, "def {reducer_function_name}(client: SpacetimeDBClient, *args) -> None:");
            writeln!(out, "    \"\"\"Call the {reducer_name} reducer.\"\"\"");
            writeln!(out, "    client._reducer_call(\"{reducer_name}\", *args)");
        }

        output.into_inner()
    }

    fn generate_globals(&self, module: &ModuleDef) -> Vec<(String, String)> {
        let mut output = CodeIndenter::new(String::new(), INDENT);
        let out = &mut output;

        print_file_header(out);

        out.newline();

        writeln!(out, "# Import and reexport all reducer arg types");
        for reducer in iter_reducers(module) {
            let reducer_name = &reducer.name;
            let reducer_module_name = reducer_module_name(reducer_name);
            let args_type = reducer_args_type_name(&reducer.name);
            writeln!(out, "from .{reducer_module_name} import {args_type}");
            if is_reducer_invokable(reducer) {
                let reducer_function_name = reducer_function_name(reducer);
                writeln!(out, "from .{reducer_module_name} import {reducer_function_name}");
            }
        }

        writeln!(out);
        writeln!(out, "# Import and reexport all table handle types");
        for table in iter_tables(module) {
            let table_name = &table.name;
            let table_module_name = table_module_name(table_name);
            let table_name_pascalcase = table.name.deref().to_case(Case::Pascal);
            let table_class = table_name_pascalcase.clone() + "Table";
            writeln!(out, "from .{table_module_name} import {table_class}");
        }

        writeln!(out);
        writeln!(out, "# Import and reexport all types");
        for ty in iter_types(module) {
            let type_name = collect_case(Case::Pascal, ty.name.name_segments());
            let type_module_name = type_module_name(&ty.name);
            writeln!(out, "from .{type_module_name} import {type_name}");
        }

        out.newline();

        // Define SpacetimeModule class
        writeln!(out, "from typing import Dict, Any");
        writeln!(out, "from spacetimedb_sdk.spacetimedb_client import SpacetimeDBClient");
        writeln!(out);
        writeln!(out, "class SpacetimeModule:");
        writeln!(out, "    \"\"\"Main module interface for SpacetimeDB.\"\"\"");
        writeln!(out, "    ");
        writeln!(out, "    def __init__(self, client: SpacetimeDBClient):");
        writeln!(out, "        self.client = client");
        writeln!(out, "        ");
        writeln!(out, "        # Initialize table handles");
        for table in iter_tables(module) {
            let table_name = table.name.deref();
            let table_name_snake = table.name.deref().to_case(Case::Snake);
            let table_name_pascalcase = table.name.deref().to_case(Case::Pascal);
            let table_class = table_name_pascalcase.clone() + "Table";
            writeln!(out, "        self.{table_name_snake} = {table_class}(client)");
        }

        writeln!(out, "        ");
        writeln!(out, "        # Initialize reducer functions");
        for reducer in iter_reducers(module).filter(|r| is_reducer_invokable(r)) {
            let reducer_name = &reducer.name;
            let reducer_function_name = reducer_function_name(reducer);
            writeln!(out, "        self.{reducer_function_name} = lambda *args: {reducer_function_name}(client, *args)");
        }

        writeln!(out, "    ");
        writeln!(out, "    @staticmethod");
        writeln!(out, "    def builder():");
        writeln!(out, "        \"\"\"Create a new SpacetimeModule builder.\"\"\"");
        writeln!(out, "        return SpacetimeModuleBuilder()");

        writeln!(out);
        writeln!(out, "class SpacetimeModuleBuilder:");
        writeln!(out, "    \"\"\"Builder for SpacetimeModule connections.\"\"\"");
        writeln!(out, "    ");
        writeln!(out, "    def __init__(self):");
        writeln!(out, "        self._auth_token = None");
        writeln!(out, "        self._host = None");
        writeln!(out, "        self._address_or_name = None");
        writeln!(out, "        self._ssl_enabled = True");
        writeln!(out, "        self._on_connect = None");
        writeln!(out, "        self._on_disconnect = None");
        writeln!(out, "        self._on_identity = None");
        writeln!(out, "        self._on_error = None");
        writeln!(out, "    ");
        writeln!(out, "    def with_credentials(self, auth_token: str):");
        writeln!(out, "        \"\"\"Set authentication token.\"\"\"");
        writeln!(out, "        self._auth_token = auth_token");
        writeln!(out, "        return self");
        writeln!(out, "    ");
        writeln!(out, "    def with_host(self, host: str):");
        writeln!(out, "        \"\"\"Set the host address.\"\"\"");
        writeln!(out, "        self._host = host");
        writeln!(out, "        return self");
        writeln!(out, "    ");
        writeln!(out, "    def with_database(self, address_or_name: str):");
        writeln!(out, "        \"\"\"Set the database address or name.\"\"\"");
        writeln!(out, "        self._address_or_name = address_or_name");
        writeln!(out, "        return self");
        writeln!(out, "    ");
        writeln!(out, "    def with_ssl(self, enabled: bool = True):");
        writeln!(out, "        \"\"\"Enable or disable SSL.\"\"\"");
        writeln!(out, "        self._ssl_enabled = enabled");
        writeln!(out, "        return self");
        writeln!(out, "    ");
        writeln!(out, "    def on_connect(self, callback):");
        writeln!(out, "        \"\"\"Set connection callback.\"\"\"");
        writeln!(out, "        self._on_connect = callback");
        writeln!(out, "        return self");
        writeln!(out, "    ");
        writeln!(out, "    def on_disconnect(self, callback):");
        writeln!(out, "        \"\"\"Set disconnection callback.\"\"\"");
        writeln!(out, "        self._on_disconnect = callback");
        writeln!(out, "        return self");
        writeln!(out, "    ");
        writeln!(out, "    def on_identity(self, callback):");
        writeln!(out, "        \"\"\"Set identity callback.\"\"\"");
        writeln!(out, "        self._on_identity = callback");
        writeln!(out, "        return self");
        writeln!(out, "    ");
        writeln!(out, "    def on_error(self, callback):");
        writeln!(out, "        \"\"\"Set error callback.\"\"\"");
        writeln!(out, "        self._on_error = callback");
        writeln!(out, "        return self");
        writeln!(out, "    ");
        writeln!(out, "    def build(self) -> SpacetimeModule:");
        writeln!(out, "        \"\"\"Build and connect to SpacetimeDB.\"\"\"");
        writeln!(out, "        import spacetimedb_sdk");
        writeln!(out, "        client = spacetimedb_sdk.SpacetimeDBClient.init(");
        writeln!(out, "            auth_token=self._auth_token,");
        writeln!(out, "            host=self._host,");
        writeln!(out, "            address_or_name=self._address_or_name,");
        writeln!(out, "            ssl_enabled=self._ssl_enabled,");
        writeln!(out, "            autogen_package=None,  # Will be set by the SDK");
        writeln!(out, "            on_connect=self._on_connect,");
        writeln!(out, "            on_disconnect=self._on_disconnect,");
        writeln!(out, "            on_identity=self._on_identity,");
        writeln!(out, "            on_error=self._on_error,");
        writeln!(out, "        )");
        writeln!(out, "        return SpacetimeModule(client)");

        vec![("__init__.py".to_string(), (output.into_inner()))]
    }
}

fn print_file_header(output: &mut Indenter) {
    print_auto_generated_file_comment(output);
    writeln!(output, "# pylint: disable=all");
    writeln!(output, "# type: ignore");
}

fn define_product_type(
    module: &ModuleDef,
    out: &mut Indenter,
    name: &str,
    elements: &[(Identifier, AlgebraicTypeUse)],
) {
    writeln!(out, "from typing import NamedTuple");
    writeln!(out);
    writeln!(out, "class {name}(NamedTuple):");
    writeln!(out, "    \"\"\"Generated type for {name}.\"\"\"");
    
    if elements.is_empty() {
        writeln!(out, "    pass");
    } else {
        for (ident, ty) in elements {
            let field_name = ident.deref().to_case(Case::Snake);
            let field_type = type_name(module, ty);
            writeln!(out, "    {field_name}: {field_type}");
        }
    }
    
    writeln!(out);
}

fn define_sum_type(
    module: &ModuleDef,
    out: &mut Indenter,
    name: &str,
    variants: &[(Identifier, AlgebraicTypeUse)],
) {
    writeln!(out, "from typing import Union, Literal");
    writeln!(out, "from dataclasses import dataclass");
    writeln!(out);
    
    // Define variant classes
    for (ident, ty) in variants {
        let variant_name = ident.deref().to_case(Case::Pascal);
        writeln!(out, "@dataclass");
        writeln!(out, "class {variant_name}:");
        writeln!(out, "    \"\"\"Variant {variant_name} of {name}.\"\"\"");
        writeln!(out, "    tag: Literal[\"{variant_name}\"] = \"{variant_name}\"");
        
        if !matches!(ty, AlgebraicTypeUse::Unit) {
            let field_type = type_name(module, ty);
            writeln!(out, "    value: {field_type}");
        }
        writeln!(out);
    }
    
    // Define union type
    let variant_names: Vec<String> = variants
        .iter()
        .map(|(ident, _)| ident.deref().to_case(Case::Pascal))
        .collect();
    let union_type = variant_names.join(", ");
    
    writeln!(out, "# Union type for all variants");
    writeln!(out, "{name} = Union[{union_type}]");
    writeln!(out);
}

fn define_enum_type(
    module: &ModuleDef,
    out: &mut Indenter,
    name: &str,
    variants: &[(Identifier, AlgebraicTypeUse)],
) {
    writeln!(out, "from enum import Enum");
    writeln!(out);
    writeln!(out, "class {name}(Enum):");
    writeln!(out, "    \"\"\"Generated enum for {name}.\"\"\"");
    
    for (i, (ident, _)) in variants.iter().enumerate() {
        let variant_name = ident.deref().to_case(Case::UpperSnake);
        writeln!(out, "    {variant_name} = {i}");
    }
    
    writeln!(out);
}

fn type_ref_module_name(module: &ModuleDef, type_ref: AlgebraicTypeRef) -> String {
    let (name, _) = module.type_def_from_ref(type_ref).unwrap();
    type_module_name(name)
}

fn type_module_name(type_name: &ScopedTypeName) -> String {
    collect_case(Case::Snake, type_name.name_segments()) + "_type"
}

fn table_module_name(table_name: &Identifier) -> String {
    table_name.deref().to_case(Case::Snake) + "_table"
}

fn reducer_args_type_name(reducer_name: &Identifier) -> String {
    reducer_name.deref().to_case(Case::Pascal) + "Args"
}

fn reducer_module_name(reducer_name: &Identifier) -> String {
    reducer_name.deref().to_case(Case::Snake) + "_reducer"
}

fn reducer_function_name(reducer: &ReducerDef) -> String {
    reducer.name.deref().to_case(Case::Snake)
}

pub fn type_name(module: &ModuleDef, ty: &AlgebraicTypeUse) -> String {
    let mut s = String::new();
    write_type(module, &mut s, ty).unwrap();
    s
}

pub fn write_type<W: Write>(
    module: &ModuleDef,
    out: &mut W,
    ty: &AlgebraicTypeUse,
) -> fmt::Result {
    match ty {
        AlgebraicTypeUse::Unit => write!(out, "None")?,
        AlgebraicTypeUse::Never => write!(out, "NoReturn")?,
        AlgebraicTypeUse::Identity => write!(out, "Identity")?,
        AlgebraicTypeUse::ConnectionId => write!(out, "ConnectionId")?,
        AlgebraicTypeUse::Timestamp => write!(out, "Timestamp")?,
        AlgebraicTypeUse::TimeDuration => write!(out, "TimeDuration")?,
        AlgebraicTypeUse::ScheduleAt => write!(out, "ScheduleAt")?,
        AlgebraicTypeUse::Option(inner_ty) => {
            write!(out, "Optional[")?;
            write_type(module, out, inner_ty)?;
            write!(out, "]")?;
        }
        AlgebraicTypeUse::Primitive(prim) => match prim {
            PrimitiveType::Bool => write!(out, "bool")?,
            PrimitiveType::I8 => write!(out, "int")?,
            PrimitiveType::U8 => write!(out, "int")?,
            PrimitiveType::I16 => write!(out, "int")?,
            PrimitiveType::U16 => write!(out, "int")?,
            PrimitiveType::I32 => write!(out, "int")?,
            PrimitiveType::U32 => write!(out, "int")?,
            PrimitiveType::I64 => write!(out, "int")?,
            PrimitiveType::U64 => write!(out, "int")?,
            PrimitiveType::I128 => write!(out, "int")?,
            PrimitiveType::U128 => write!(out, "int")?,
            PrimitiveType::I256 => write!(out, "int")?,
            PrimitiveType::U256 => write!(out, "int")?,
            PrimitiveType::F32 => write!(out, "float")?,
            PrimitiveType::F64 => write!(out, "float")?,
        },
        AlgebraicTypeUse::String => write!(out, "str")?,
        AlgebraicTypeUse::Array(elem_ty) => {
            if matches!(&**elem_ty, AlgebraicTypeUse::Primitive(PrimitiveType::U8)) {
                return write!(out, "bytes");
            }
            write!(out, "List[")?;
            write_type(module, out, elem_ty)?;
            write!(out, "]")?;
        }
        AlgebraicTypeUse::Ref(r) => {
            write!(out, "{}", type_ref_name(module, *r))?;
        }
    }
    Ok(())
}

/// Print imports for each of the `imports`.
fn print_imports(module: &ModuleDef, out: &mut Indenter, imports: Imports) {
    for typeref in imports {
        let module_name = type_ref_module_name(module, typeref);
        let type_name = type_ref_name(module, typeref);
        writeln!(out, "from .{module_name} import {type_name}");
    }
}

/// Use `search_function` on `roots` to detect required imports, then print them with `print_imports`.
fn gen_and_print_imports(
    module: &ModuleDef,
    out: &mut Indenter,
    roots: &[(Identifier, AlgebraicTypeUse)],
    dont_import: &[AlgebraicTypeRef],
) {
    let mut imports = BTreeSet::new();

    for (_, ty) in roots {
        ty.for_each_ref(|r| {
            imports.insert(r);
        });
    }
    for skip in dont_import {
        imports.remove(skip);
    }
    let len = imports.len();

    print_imports(module, out, imports);

    if len > 0 {
        out.newline();
    }
}
