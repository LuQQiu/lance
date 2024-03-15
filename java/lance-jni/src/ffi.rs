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

use jni::objects::JString;
use jni::{objects::JObject, JNIEnv};

use crate::Result;

/// Extend JNIEnv with helper functions.
pub trait JNIEnvExt {
    /// Get strings from Java List<String> object.
    fn get_strings(&mut self, obj: &JObject) -> Result<Vec<String>>;

    /// Get Option<Vec<String>> from Java Optional<List<String>>.
    fn get_strings_opt(&mut self, obj: &JObject) -> Result<Option<Vec<String>>>;

    /// Get Option<i32> from Java Optional<Integer>.
    fn get_int_opt(&mut self, obj: &JObject) -> Result<Option<i32>>;

    /// Get Option<i64> from Java Optional<Long>.
    fn get_long_opt(&mut self, obj: &JObject) -> Result<Option<i64>>;

    /// Get Option<String> from Java Optional<String>.
    fn get_string_opt(&mut self, obj: &JObject) -> Result<Option<String>>;
}

// TODO(lu) Consolidate
impl JNIEnvExt for JNIEnv<'_> {
    fn get_strings(&mut self, obj: &JObject) -> Result<Vec<String>> {
        let list = self.get_list(obj)?;
        let mut iter = list.iter(self)?;
        let mut results = Vec::with_capacity(list.size(self)? as usize);
        while let Some(elem) = iter.next(self)? {
            let jstr = JString::from(elem);
            let val = self.get_string(&jstr)?;
            results.push(val.to_str()?.to_string())
        }
        Ok(results)
    }

    fn get_strings_opt(&mut self, obj: &JObject) -> Result<Option<Vec<String>>> {
        if obj.is_null() {
            return Ok(None);
        }
        let is_empty = self.call_method(obj, "isEmpty", "()Z", &[])?;
        if is_empty.z()? {
            Ok(None)
        } else {
            let inner = self.call_method(obj, "get", "()Ljava/util/List;", &[])?;
            let inner_obj = inner.l()?;
            Ok(Some(self.get_strings(&inner_obj)?))
        }
    }

    fn get_int_opt(&mut self, obj: &JObject) -> Result<Option<i32>> {
        if obj.is_null() {
            return Ok(None);
        }
        let is_present = self.call_method(obj, "isPresent", "()Z", &[])?;
        if is_present.z()? {
            let value = self.call_method(obj, "get", "()Ljava/lang/Integer;", &[])?;
            let value_obj = value.l()?;
            let int_value = self.call_method(value_obj, "intValue", "()I", &[])?.i()?;
            Ok(Some(int_value))
        } else {
            Ok(None)
        }
    }

       /// Get Option<i64> from Java Optional<Long>.
    fn get_long_opt(&mut self, obj: &JObject) -> Result<Option<i64>> {
        if obj.is_null() {
            return Ok(None);
        }
        let is_present = self.call_method(obj, "isPresent", "()Z", &[])?;
        if is_present.z()? {
            let value = self.call_method(obj, "get", "()Ljava/lang/Long;", &[])?;
            let value_obj = value.l()?;
            let long_value = self.call_method(value_obj, "longValue", "()J", &[])?.j()?;
            Ok(Some(long_value))
        } else {
            Ok(None)
        }
    }

    /// Get Option<String> from Java Optional<String>.
    fn get_string_opt(&mut self, obj: &JObject) -> Result<Option<String>> {
        if obj.is_null() {
            return Ok(None);
        }
        let is_present = self.call_method(obj, "isPresent", "()Z", &[])?;
        if is_present.z()? {
            let value = self.call_method(obj, "get", "()Ljava/lang/String;", &[])?;
            let value_obj = value.l()?;
            let string_value = self.get_string(&JString::from(value_obj))?.into();
            Ok(Some(string_value))
        } else {
            Ok(None)
        }
    }
}
