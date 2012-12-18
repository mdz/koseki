require 'sinatra/base'
require 'json'

module Koseki
  class Web < Sinatra::Base
    helpers do
      raise "No API_KEY specified" unless ENV['API_KEY']
      set :port, ENV['PORT']

      def protected_by_api_key!
        unless http_basic_authorized?
          response['WWW-Authenticate'] = %(Basic realm="Restricted Area")
          throw(:halt, [401, "Not authorized\n"])
        end
      end

      def http_basic_authorized?
        @auth ||=  Rack::Auth::Basic::Request.new(request.env)
        @auth.provided? && @auth.basic? && @auth.credentials && ENV['API_KEY'] && @auth.credentials[1] == ENV['API_KEY']
      end
    end

    post "/register-cloud" do
      protected_by_api_key!
      if not Koseki::Cloud.where(Sequel.or({:name => params['name'], :account_number => params['account_number']})).empty?
        status 400
        return JSON.dump({"error" => "A cloud already exists with the specified name or account number"})
      end

      begin
        Koseki::Cloud.register(params)
      rescue RuntimeError => err
        status 500
        return JSON.dump({"error" => err})
      end

      status 200
      JSON.dump({"status" => "ok"})
    end
  end
end
