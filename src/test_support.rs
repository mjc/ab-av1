pub(crate) fn assert_no_allocations(run: impl FnOnce()) {
    let allocations = allocation_counter::measure(run);

    assert_eq!(allocations.count_total, 0);
}
