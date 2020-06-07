import XCTest

#if !canImport(ObjectiveC)
public func allTests() -> [XCTestCaseEntry] {
    return [
        testCase(helloworld_projectTests.allTests),
    ]
}
#endif
