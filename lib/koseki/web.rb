require 'sinatra/base'
require 'json'

module Koseki
  class Web < Sinatra::Base

    post "/register-cloud" do
      if not Koseki::Cloud.where(Sequel.or({:name => params['name'], :account_number => params['account_number']})).empty?
        status 400
        return JSON.dump({"error" => "A cloud already exists with the specified name or account number"})
      end

      Koseki::Cloud.register(params)

      status 200
      JSON.dump({"status" => "ok"})
    end
  end
end
