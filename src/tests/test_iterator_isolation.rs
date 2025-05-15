use crate::{iterators::StorageIterator, mem_table::MemTable};

#[test]
fn test_scan_concurrent_insert() {
    use std::ops::Bound;
    let memtable = MemTable::create(0);
    memtable.for_testing_put_slice(b"key1", b"value1").unwrap();
    memtable.for_testing_put_slice(b"key2", b"value2").unwrap();
    memtable.for_testing_put_slice(b"key3", b"value3").unwrap();

    let mut iter = memtable.for_testing_scan_slice(Bound::Unbounded, Bound::Unbounded);
    assert_eq!(iter.key().for_testing_key_ref(), b"key1");
    assert_eq!(iter.value(), b"value1");
    assert!(iter.is_valid());
    iter.next().unwrap();
    assert_eq!(iter.key().for_testing_key_ref(), b"key2");
    assert_eq!(iter.value(), b"value2");
    assert!(iter.is_valid());
    iter.next().unwrap();
    assert_eq!(iter.key().for_testing_key_ref(), b"key3");
    assert_eq!(iter.value(), b"value3");
    assert!(iter.is_valid());
    iter.next().unwrap();
    assert!(!iter.is_valid());
    memtable.for_testing_put_slice(b"key4", b"value4").unwrap();
    iter.next().unwrap();
    assert!(iter.is_valid());
    assert_eq!(iter.key().for_testing_key_ref(), b"key4");
    assert_eq!(iter.value(), b"value4");
    iter.next().unwrap();
    assert!(!iter.is_valid());
}
#[test]
fn test_scan_range() {
    // use std::ops::Bound;
    let memtable = MemTable::create(0);
    memtable.for_testing_put_slice(b"key1", b"value1").unwrap();
    memtable.for_testing_put_slice(b"key2", b"value2").unwrap();
    memtable.for_testing_put_slice(b"key3", b"value3").unwrap();

    {
        let mut iter = memtable.for_testing_scan_range_slice(..);
        assert_eq!(iter.key().for_testing_key_ref(), b"key1");
        assert_eq!(iter.value(), b"value1");
        assert!(iter.is_valid());
        iter.next().unwrap();
        assert_eq!(iter.key().for_testing_key_ref(), b"key2");
        assert_eq!(iter.value(), b"value2");
        assert!(iter.is_valid());
        iter.next().unwrap();
        assert_eq!(iter.key().for_testing_key_ref(), b"key3");
        assert_eq!(iter.value(), b"value3");
        assert!(iter.is_valid());
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }

    {
        let mut iter = memtable.for_testing_scan_range_slice(b"key1"..b"key2");
        assert_eq!(iter.key().for_testing_key_ref(), b"key1");
        assert_eq!(iter.value(), b"value1");
        assert!(iter.is_valid());
        iter.next().unwrap();
        // assert_eq!(iter.key().for_testing_key_ref(), b"key2");
        // assert_eq!(iter.value(), b"value2");
        // assert!(iter.is_valid());
        // iter.next().unwrap();
        assert!(!iter.is_valid());
    }
    {
        let mut iter = memtable.for_testing_scan_range_slice(b"key1"..=b"key2");
        assert_eq!(iter.key().for_testing_key_ref(), b"key1");
        assert_eq!(iter.value(), b"value1");
        assert!(iter.is_valid());
        iter.next().unwrap();
        assert_eq!(iter.key().for_testing_key_ref(), b"key2");
        assert_eq!(iter.value(), b"value2");
        assert!(iter.is_valid());
        iter.next().unwrap();
        assert!(!iter.is_valid());
    }
}
