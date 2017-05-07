require_relative '../lib/mongo_repository'
require 'test/unit'

TEST_DATA = [
    {int: 1, string: 'XXX'},
    {int: 2, string: 'ZZZ'},
    {int: 3, string: 'ZZZ'}
]
class MongoRepositoryTest < Test::Unit::TestCase
  COLLECTION = 'test'
  def setup
    @sut = MongoRepository.new('localhost', 'ruby_test')
    assert(!@sut.nil?)
    @sut.drop
  end

  def test_database_drop
    @sut.add(COLLECTION,TEST_DATA.first)
    assert_equal(1, @sut.collection(COLLECTION).count)
    @sut.drop
    assert_equal(0, @sut.collection(COLLECTION).count)
  end

  def test_collection_drop
    @sut.add(COLLECTION,TEST_DATA)
    sut_collection = @sut.collection(COLLECTION)
    assert_equal(3, sut_collection.count)
    sut_collection.drop
    assert_equal(0, sut_collection.count)
  end

  def test_add
    assert_equal(1, @sut.add(COLLECTION,TEST_DATA.first))
    assert_equal(2,  @sut.add(COLLECTION,
                              [TEST_DATA[1], TEST_DATA[2]]))
  end

  def test_find
    assert_equal(3, @sut.add(COLLECTION, TEST_DATA))
    # All documents
    assert_equal(3, @sut.find(COLLECTION).count)

    result = @sut.find(COLLECTION, string: 'XXX')
    assert_equal(1, result.count)
    assert_equal(1, result.first[:int])

    result = @sut.find(COLLECTION, string: 'ZZZ')
    assert_equal(2, result.count)
    assert_equal(2, result.first[:int])
  end

end