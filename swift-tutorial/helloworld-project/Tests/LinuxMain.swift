import XCTest

import helloworld_Tests

var tests = [XCTestCaseEntry]()
tests += helloworld_Tests.allTests()
XCTMain(tests)
