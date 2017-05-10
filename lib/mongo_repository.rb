require 'mongo'

Mongo::Logger.logger.level = ::Logger::INFO

class MongoRepository
  def initialize(host, database, port = 27017)
    @client = Mongo::Client.new([ "#{host}:#{port}" ], database: database)
  end

  def drop
    @client.database.drop
  end

  def collection(name)
    @client[name.to_sym]
  end

  def add(collection, data)
    col = collection(collection)
    return col.insert_many(data).inserted_count if data.kind_of?(Array)
    col.insert_one(data).n
  end

  def find(collection, query = nil)
    collection(collection).find(query)
  end

  def replace(collection, query, data)
    find_one_and_replace(find(collection, query), data)
  end

  # Only supports single documents
  def create_or_replace(collection, query, data)
    raise "create_or_replace: data can not be an array!" if data.kind_of?(Array)
    found = find(collection, query)
    raise "create_or_replace only supports a single document. #{query} found #{found.count} results!" if found.count > 1
    if found.count == 0
      add(collection, data)
      find(collection, query).first
    else
      find_one_and_replace(found, data)
    end
  end

  private
  def find_one_and_replace(find_result, data)
    find_result.find_one_and_replace(data, return_document: :after)
  end

end