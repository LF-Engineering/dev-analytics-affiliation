#!/usr/bin/env ruby

require 'jwt'
require 'net/http'

JWT_TOKEN       = ENV.fetch('JWT_TOKEN', '')
JWT_ALGORITHM   = ENV.fetch('JWT_ALGORITHM', '')
AUTH0_CLIENT_ID = ENV.fetch('AUTH0_CLIENT_ID', '')
AUTH0_DOMAIN    = ENV.fetch('AUTH0_DOMAIN', '')
KEY_ID          = ENV.fetch('KEY_ID', '')
JWT_PATH        = ENV.fetch('JWT_PATH', '')
JWT_KEYS        = ENV.fetch('JWT_KEYS', '')
JWT_CERT_KEY    = ENV.fetch('JWT_CERT_KEY', '')

def public_key(k)
  OpenSSL::X509::Certificate.new(Base64.decode64(k[JWT_CERT_KEY].first)).public_key
end

def extract_key_pair(jwks_item)
  [ jwks_item[KEY_ID], public_key(jwks_item) ]
end

def acquire_keys(auth_domain)
  auth0_jwks = ::Net::HTTP.get(URI("https://#{auth_domain}/#{JWT_PATH}"))
  jwks_keys = JSON.parse(auth0_jwks)[JWT_KEYS]
  Hash[
    jwks_keys.map { |k| extract_key_pair(k) }
  ]
end

def jwks
  acquire_keys(AUTH0_DOMAIN)
end

def decode(token)
  payload, header = decode_jwt(token)
  payload
end

def decode_jwt(token)
  issuer = "https://#{AUTH0_DOMAIN}/"
  JWT.decode(token, nil, true,
    algorithm: JWT_ALGORITHM,
    iss: issuer,
    aud: AUTH0_CLIENT_ID,
    verify_iss: true,
    verify_aud: true,
    verify_iat: true,
  ) { |header| jwks[header[KEY_ID]] }
end

p jwks()

token = JWT.decode(JWT_TOKEN, nil, false)
p token
p jwks.values.first

token = decode_jwt(JWT_TOKEN)
p token
