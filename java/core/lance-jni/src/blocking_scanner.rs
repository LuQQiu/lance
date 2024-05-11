// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use crate::error::{JavaErrorExt, JavaResult};
use crate::ffi::JNIEnvExt;
use crate::JavaError;
use arrow::{ffi::FFI_ArrowSchema, ffi_stream::FFI_ArrowArrayStream};
use arrow_schema::SchemaRef;
use jni::{objects::JObject, sys::jlong, JNIEnv};
use lance::dataset::scanner::{DatasetRecordBatchStream, Scanner};
use lance_io::ffi::to_ffi_arrow_array_stream;

use crate::{
    blocking_dataset::{BlockingDataset, NATIVE_DATASET},
    traits::IntoJava,
    RT,
};

pub const NATIVE_SCANNER: &str = "nativeScannerHandle";

#[derive(Clone)]
pub struct BlockingScanner {
    pub(crate) inner: Arc<Scanner>,
}

impl BlockingScanner {
    pub fn create(scanner: Scanner) -> Self {
        Self {
            inner: Arc::new(scanner),
        }
    }

    pub fn open_stream(&self) -> JavaResult<DatasetRecordBatchStream> {
        RT.block_on(self.inner.try_into_stream()).infer_error()
    }

    pub fn schema(&self) -> JavaResult<SchemaRef> {
        RT.block_on(self.inner.schema()).infer_error()
    }

    pub fn count_rows(&self) -> JavaResult<u64> {
        RT.block_on(self.inner.count_rows()).infer_error()
    }
}

///////////////////
// Write Methods //
///////////////////
#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_ipc_LanceScanner_createScanner<'local>(
    mut env: JNIEnv<'local>,
    _reader: JObject,
    jdataset: JObject,
    fragment_ids_obj: JObject,     // Optional<List<Integer>>
    columns_obj: JObject,          // Optional<List<String>>
    substrait_filter_obj: JObject, // Optional<ByteBuffer>
    filter_obj: JObject,           // Optional<String>
    batch_size_obj: JObject,       // Optional<Long>
) -> JObject<'local> {
    ok_or_throw!(
        env,
        inner_create_scanner(
            &mut env,
            jdataset,
            fragment_ids_obj,
            columns_obj,
            substrait_filter_obj,
            filter_obj,
            batch_size_obj
        )
    )
}

fn inner_create_scanner<'local>(
    env: &mut JNIEnv<'local>,
    jdataset: JObject,
    fragment_ids_obj: JObject,
    columns_obj: JObject,
    substrait_filter_obj: JObject,
    filter_obj: JObject,
    batch_size_obj: JObject,
) -> JavaResult<JObject<'local>> {
    let dataset = {
        let dataset =
            unsafe { env.get_rust_field::<_, _, BlockingDataset>(jdataset, NATIVE_DATASET) }
                .infer_error()?;
        dataset.clone()
    };
    let mut scanner = dataset.inner.scan();
    let fragment_ids_opt = env.get_ints_opt(&fragment_ids_obj)?;
    if let Some(fragment_ids) = fragment_ids_opt {
        let mut fragments = Vec::with_capacity(fragment_ids.len());
        for fragment_id in fragment_ids {
            let Some(fragment) = dataset.inner.get_fragment(fragment_id as usize) else {
                return Err(JavaError::input_error(format!(
                    "Fragment {fragment_id} not found"
                )));
            };
            fragments.push(fragment.metadata().clone());
        }
        scanner.with_fragments(fragments);
    }
    let columns_opt = env.get_strings_opt(&columns_obj)?;
    if let Some(columns) = columns_opt {
        scanner.project(&columns).infer_error()?;
    };
    let substrait_opt = env.get_bytes_opt(&substrait_filter_obj)?;
    if let Some(substrait) = substrait_opt {
        RT.block_on(async { scanner.filter_substrait(substrait).await })
            .infer_error()?;
    }
    let filter_opt = env.get_string_opt(&filter_obj)?;
    if let Some(filter) = filter_opt {
        scanner.filter(filter.as_str()).infer_error()?;
    }
    let batch_size_opt = env.get_long_opt(&batch_size_obj)?;
    if let Some(batch_size) = batch_size_opt {
        scanner.batch_size(batch_size as usize);
    }
    let scanner = BlockingScanner::create(scanner);
    scanner.into_java(env)
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_ipc_LanceScanner_releaseNativeScanner(
    mut env: JNIEnv,
    j_scanner: JObject,
) {
    ok_or_throw_without_return!(env, inner_release_native_scanner(&mut env, j_scanner));
}

fn inner_release_native_scanner(env: &mut JNIEnv, j_scanner: JObject) -> JavaResult<()> {
    let _: BlockingScanner =
        unsafe { env.take_rust_field(j_scanner, NATIVE_SCANNER) }.infer_error()?;
    Ok(())
}

impl IntoJava for BlockingScanner {
    fn into_java<'local>(self, env: &mut JNIEnv<'local>) -> JavaResult<JObject<'local>> {
        attach_native_scanner(env, self)
    }
}

fn attach_native_scanner<'local>(
    env: &mut JNIEnv<'local>,
    scanner: BlockingScanner,
) -> JavaResult<JObject<'local>> {
    let j_scanner = create_java_scanner_object(env)?;
    // This block sets a native Rust object (scanner) as a field in the Java object (j_scanner).
    // Caution: This creates a potential for memory leaks. The Rust object (scanner) is not
    // automatically garbage-collected by Java, and its memory will not be freed unless
    // explicitly handled.
    //
    // To prevent memory leaks, ensure the following:
    // 1. The Java object (`j_scanner`) should implement the `java.io.Closeable` interface.
    // 2. Users of this Java object should be instructed to always use it within a try-with-resources
    //    statement (or manually call the `close()` method) to ensure that `self.close()` is invoked.
    unsafe { env.set_rust_field(&j_scanner, NATIVE_SCANNER, scanner) }.infer_error()?;
    Ok(j_scanner)
}

fn create_java_scanner_object<'a>(env: &mut JNIEnv<'a>) -> JavaResult<JObject<'a>> {
    env.new_object("com/lancedb/lance/ipc/LanceScanner", "()V", &[])
        .infer_error()
}

//////////////////
// Read Methods //
//////////////////
#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_ipc_LanceScanner_openStream(
    mut env: JNIEnv,
    j_scanner: JObject,
    stream_addr: jlong,
) {
    ok_or_throw_without_return!(env, inner_open_stream(&mut env, j_scanner, stream_addr));
}

fn inner_open_stream(env: &mut JNIEnv, j_scanner: JObject, stream_addr: jlong) -> JavaResult<()> {
    let scanner = {
        let scanner_guard =
            unsafe { env.get_rust_field::<_, _, BlockingScanner>(j_scanner, NATIVE_SCANNER) }
                .infer_error()?;
        scanner_guard.clone()
    };
    let record_batch_stream = scanner.open_stream()?;
    let ffi_stream =
        to_ffi_arrow_array_stream(record_batch_stream, RT.handle().clone()).infer_error()?;
    unsafe { std::ptr::write_unaligned(stream_addr as *mut FFI_ArrowArrayStream, ffi_stream) }
    Ok(())
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_ipc_LanceScanner_importFfiSchema(
    mut env: JNIEnv,
    j_scanner: JObject,
    schema_addr: jlong,
) {
    ok_or_throw_without_return!(
        env,
        inner_import_ffi_schema(&mut env, j_scanner, schema_addr)
    );
}

fn inner_import_ffi_schema(
    env: &mut JNIEnv,
    j_scanner: JObject,
    schema_addr: jlong,
) -> JavaResult<()> {
    let scanner = {
        let scanner_guard =
            unsafe { env.get_rust_field::<_, _, BlockingScanner>(j_scanner, NATIVE_SCANNER) }
                .infer_error()?;
        scanner_guard.clone()
    };
    let schema = scanner.schema()?;
    let ffi_schema = FFI_ArrowSchema::try_from(&*schema).infer_error()?;
    unsafe { std::ptr::write_unaligned(schema_addr as *mut FFI_ArrowSchema, ffi_schema) }
    Ok(())
}

#[no_mangle]
pub extern "system" fn Java_com_lancedb_lance_ipc_LanceScanner_countRows(
    mut env: JNIEnv,
    j_scanner: JObject,
) -> jlong {
    ok_or_throw_with_return!(env, inner_count_rows(&mut env, j_scanner), -1) as jlong
}

fn inner_count_rows(env: &mut JNIEnv, j_scanner: JObject) -> JavaResult<u64> {
    let scanner = {
        let scanner_guard =
            unsafe { env.get_rust_field::<_, _, BlockingScanner>(j_scanner, NATIVE_SCANNER) }
                .infer_error()?;
        scanner_guard.clone()
    };
    scanner.count_rows()
}
