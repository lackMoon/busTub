#include <fmt/format.h>
#include <fmt/ranges.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <atomic>
#include <chrono>  // NOLINT
#include <cstdio>
#include <memory>
#include <random>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>  //NOLINT
#include <utility>
#include <vector>

#include "common_checker.h"  // NOLINT

namespace bustub {

void CommitTest1() {
  // should scan changes of committed txn
  auto db = GetDbForCommitAbortTest("CommitTest1");
  auto txn1 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Insert(txn1, *db, 1);
  Commit(*db, txn1);
  auto txn2 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Scan(txn2, *db, {1, 233, 234});
  Commit(*db, txn2);
}

// NOLINTNEXTLINE
TEST(CommitAbortTest, CommitTestA) { CommitTest1(); }

void Test1(IsolationLevel lvl) {
  // should scan changes of committed txn
  auto db = GetDbForVisibilityTest("Test1");
  auto txn1 = Begin(*db, lvl);
  Delete(txn1, *db, 233);
  Commit(*db, txn1);
  auto txn2 = Begin(*db, lvl);
  Scan(txn2, *db, {234});
  Commit(*db, txn2);
}

// NOLINTNEXTLINE
TEST(VisibilityTest, TestA) {
  // only this one will be public :)
  Test1(IsolationLevel::READ_COMMITTED);
}

void Test2() {
  auto db = GetDbForVisibilityTest("Test2");
  auto txn1 = Begin(*db, IsolationLevel::READ_COMMITTED);
  Delete(txn1, *db, 233);
  Commit(*db, txn1);
  auto txn2 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Scan(txn2, *db, {234});
  Commit(*db, txn2);
}

// NOLINTNEXTLINE
TEST(VisibilityTest, TestB) {
  // only this one will be public :)
  Test2();
}

void Test3() {
  auto db = GetDbForVisibilityTest("Test3");
  auto txn1 = Begin(*db, IsolationLevel::READ_COMMITTED);
  Delete(txn1, *db, 233);
  Commit(*db, txn1);
  auto txn2 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Scan(txn2, *db, {234});
  Commit(*db, txn2);
}

// NOLINTNEXTLINE
TEST(VisibilityTest, TestC) {
  // only this one will be public :)
  Test3();
}

void Test4() {
  // txn1: INSERT INTO t1 VALUES (200, 20), (201, 21), (202, 22)
  // txn1: abort
  // txn2: SELECT * FROM t1;

  auto db = GetDbForPersonalTest("Test4");
  auto txn1 = Begin(*db, IsolationLevel::REPEATABLE_READ);
  Insert(txn1, *db, {200, 201, 202}, {20, 21, 22});
  Abort(*db, txn1);
  delete txn1;

  auto txn2 = Begin(*db, IsolationLevel::REPEATABLE_READ);
  Scan(txn2, *db, {});
  Commit(*db, txn2);
  delete txn2;
}

// NOLINTNEXTLINE
TEST(InsertRollbackTest, TestA) {
  // only this one will be public :)
  Test4();
}

void Test5() {
  // txn1: INSERT INTO t1 VALUES (200, 20), (201, 21), (202, 22)
  // txn2: SELECT * FROM t1;
  // txn1: abort
  auto db = GetDbForPersonalTest("Test5");
  auto txn1 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Insert(txn1, *db, 200);
  Insert(txn1, *db, 201);
  Insert(txn1, *db, 202);
  auto txn2 = Begin(*db, IsolationLevel::READ_UNCOMMITTED);
  Scan(txn2, *db, {200, 201, 202});
  Commit(*db, txn2);
  delete txn2;
  Abort(*db, txn1);
  delete txn1;
}

// NOLINTNEXTLINE
TEST(DirtyReadsTest, TestA) {
  // only this one will be public :)
  Test5();
}

void Test6() {
  auto db = GetDbForCommitAbortTest("Test6");
  auto txn1 = Begin(*db, IsolationLevel::REPEATABLE_READ);
  Update(txn1, *db, 235, 234);
  Scan(txn1, *db, {233, 235});
  Abort(*db, txn1);
  delete txn1;
  auto txn2 = Begin(*db, IsolationLevel::REPEATABLE_READ);
  Scan(txn2, *db, {233, 234});
  Commit(*db, txn2);
  delete txn2;
}

// NOLINTNEXTLINE
TEST(UpdateAbortTest, TestA) {
  // only this one will be public :)
  Test6();
}

// NOLINTNEXTLINE
TEST(IsolationLevelTest, InsertTestA) {
  ExpectTwoTxn("InsertTestA.1", IsolationLevel::READ_UNCOMMITTED, IsolationLevel::READ_UNCOMMITTED, false, IS_INSERT,
               ExpectedOutcome::DirtyRead);
}

// NOLINTNEXTLINE
TEST(IsolationLevelTest, DeleteTestA) {
  ExpectTwoTxn("DeleteTestA.1", IsolationLevel::READ_COMMITTED, IsolationLevel::READ_UNCOMMITTED, false, IS_DELETE,
               ExpectedOutcome::BlockOnRead);
}

void MixedTest(IsolationLevel lvl) {
  // should scan changes of committed txn
  auto db = GetDbForMixedTest();
  auto txn1 = Begin(*db, lvl);
  auto txn2 = Begin(*db, lvl);
  fmt::print("------------------\n");
  Delete(txn2, *db, 100);
  Scan(txn2, *db, {101, 102});
  Abort(*db, txn2);
  Scan(txn1, *db, {100, 101, 102});
  Commit(*db, txn1);
}

TEST(IsolationLevelTest, MixedTest) { MixedTest(IsolationLevel::REPEATABLE_READ); }

}  // namespace bustub
