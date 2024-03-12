pub fn get_type_of<T>(_: &T) -> &'static str {
    std::any::type_name::<T>()
}
