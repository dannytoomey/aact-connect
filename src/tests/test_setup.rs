use crate::setup::check_args::check_args;
use crate::structs::args::Args;
use crate::tests::utils::print_type_of;

#[test]
fn test_setup() {
    let test_args = Args {
        username: Some("test".to_string()),
        password: Some("test".to_string()),
        search: true,
        current_frame: true,
        existing_frame: Some("".to_string()),
        threads: 64,
    };
    let test_type = print_type_of(&test_args);
    let check = check_args(test_args);
    assert_eq!(test_type, print_type_of(&check.unwrap()))
}
