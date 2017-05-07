require 'mongo'

Mongo::Logger.logger.level = ::Logger::INFO

class MongoRepository
  def initialize(host, database, port = 27017)
    @client = Mongo::Client.new([ "#{host}:#{port}" ], :database => database)
    @db = @client.database
  end

  def drop
    @db.drop
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
end