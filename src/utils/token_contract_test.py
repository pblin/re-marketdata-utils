from web3 import Web3, HTTPProvider, IPCProvider, WebsocketProvider, middleware
from web3.gas_strategies.time_based import medium_gas_price_strategy
from web3.middleware import geth_poa_middleware
# from web3.providers.eth_tester import EthereumTesterProvider
import json, csv
import uuid
import os

with open('ReblocDatasetToken.json') as json_file:
    contract_json = json.load(json_file)

#w3 = Web3(Web3.HTTPProvider('http://127.0.0.1:8545'))
w3 = Web3(Web3.HTTPProvider('https://rinkeby.infura.io/v3/2d21141376574da189cccef28362da65'))


# print ('send 90 eth:' + str(txn_hash))

print (contract_json['abi'])
token_contract = w3.eth.contract('0x49FC8385c3BeA67B84799e4Bde1fAD7B6829526e',abi=contract_json['abi'])

token_symbol=token_contract.functions.name().call()

print (token_symbol)

operator_address = token_contract.functions.getOperatorAccount().call()
print (operator_address)
id = uuid.UUID('39112a5c-b7dc-11e9-9abf-f40f2427ca38').bytes
file_hash = 'aa5e8eb3d895a6df3678e1553b6ba066'
compression = 'gzip'
size = 979
ipfs_hash = 'QmTCTGEaaX8KL3CeNDwjkeSzunKaWhbWDghnQuDtEJvWqa'
price = 1
pricing_unit = 'usd'
token_uri = 'http://demo-app.rebloc.io:8080/ipfs/QmTCTGEaaX8KL3CeNDwjkeSzunKaWhbWDghnQuDtEJvWqa'
w3.middleware_onion.inject(geth_poa_middleware, layer=0)
gas_price=w3.eth.gasPrice
print ('gas price: %d' % gas_price)

# txn = token_contract.functions.mint(id,file_hash,compression,ipfs_hash,size,price,pricing_unit,token_uri,'0x4CA6f47262355A6843597dB41eE44091AcEf51Db' )\

buyer_account='0xF8176d60f1C641B24A8D77d939514Be73C62d818'
seller_account='0x4CA6f47262355A6843597dB41eE44091AcEf51Db'

current_balance = w3.eth.getBalance(seller_account)
print ('current accout balance %d' % current_balance )
tx_receipt = w3.eth.getTransactionReceipt('0xbee07197791fd533d8a123d68559d8391b15f24c7eaf49252e90f0040316db27')
mint_event = token_contract.events.MintToken().processReceipt(tx_receipt)
token_id = mint_event[0]['args']['_tokenId']
print ('token id = %d' % token_id)
gas = token_contract.functions.purchaseWithFiat(token_id,0,buyer_account).estimateGas({'nonce':w3.eth.getTransactionCount(seller_account),
                       'from':seller_account})

print ('estimate gas = %d' % gas)
txn = token_contract.functions.purchaseWithFiat(token_id,0,buyer_account)\
    .buildTransaction({'nonce':w3.eth.getTransactionCount(seller_account),
                       'from':seller_account,
                       'gas': gas,
                       'gasPrice': w3.eth.gasPrice})
# set priviate key
private_key = 
signed = w3.eth.account.signTransaction(txn, private_key)
txn_hash = w3.eth.sendRawTransaction(signed.rawTransaction)
print ('txn hash = %x' % txn_hash)



