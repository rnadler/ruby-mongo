require_relative '../lib/mongo_repository'
require 'test/unit'

TEST_DATA = [
    {int: 1, string: 'XXX'},
    {int: 2, string: 'ZZZ'},
    {int: 3, string: 'ZZZ'}
].freeze

class MongoRepositoryTest < Test::Unit::TestCase
  HOST = 'localhost'
  DATABASE = 'ruby_test'
  COLLECTION = 'test'

  def setup
    @sut = MongoRepository.new(HOST, DATABASE)
    assert_not_nil(@sut)
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

  def test_replace
    assert_equal(3, @sut.add(COLLECTION, TEST_DATA))
    orig = @sut.find(COLLECTION, string: 'XXX').first
    doc = @sut.replace(COLLECTION, {string: orig[:string]},  {int: 10, string: 'XXX'})
    assert_equal(1, orig[:int])
    assert_equal(10, doc[:int])
    assert_equal(3, @sut.collection(COLLECTION).count)
  end

  def test_create_or_replace
    assert_equal(1, @sut.add(COLLECTION, TEST_DATA[0]))
    orig = @sut.find(COLLECTION, string: 'XXX').first
    doc = @sut.create_or_replace(COLLECTION, {string: orig[:string]},  {int: 10, string: 'XXX'})
    assert_equal(1, orig[:int])
    assert_equal(10, doc[:int])
    assert_equal(1, @sut.collection(COLLECTION).count)
    doc = @sut.create_or_replace(COLLECTION, {int: 11},  {int: 11, string: 'XXX'})
    assert_equal(11, doc[:int])
    assert_equal(2, @sut.collection(COLLECTION).count)
    # Does not allow data to be an Array
    assert_raise RuntimeError do
      @sut.create_or_replace(COLLECTION, {}, [])
    end
    # Only supports single document updates
    assert_raise RuntimeError do
      @sut.create_or_replace(COLLECTION, {string: 'XXX'}, {})
    end
  end

end