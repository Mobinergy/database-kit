import NIOConcurrencyHelpers

final class ConcurrentDictionary<Key, Value> where Key: Hashable {
    
    init() { }
    
    @discardableResult
    func insertValue(forKey key: Key, using computeValue: () -> Value) -> Value {
        let (current, inserted): (Box, Bool) = lock.withLock {
            if let box = storage[key] {
                return (box, false)
            } else {
                let box = Box()
                storage[key] = box
                return (box, true)
            }
        }
        if inserted {
            let value = computeValue()
            current.initialize(with: value)
            return value
        } else {
            return current.get()
        }
    }
    
    @discardableResult
    public func removeAll() -> [Key: Value] {
        let boxes: [Key: Box] = lock.withLock {
            let boxes = storage
            storage = [:]
            return boxes
        }
        var removedValues: [Key: Value] = [:]
        for (key, box) in boxes {
            removedValues[key] = box.finalize()
        }
        return removedValues
    }
    
    private let lock: Lock = .init()
    private var storage: [Key: Box] = [:]
    
    private final class Box {
        private let lock: Lock = .init()
        private var value: Value? = nil
        private var isFinalized: Bool = false
        
        init() {
            // The box is locked until initialized.
            lock.lock()
        }
        
        func initialize(with value: Value) {
            assert(self.value == nil)
            self.value = value
            lock.unlock()
        }
        
        func get() -> Value {
            lock.withLock {
                // value is guaranteed to be initialized once we acquire the lock
                value!
            }
        }
        
        func finalize() -> Value? {
            lock.withLock {
                guard !isFinalized else {
                    return nil
                }
                isFinalized = true
                return value
            }
        }
    }
}
