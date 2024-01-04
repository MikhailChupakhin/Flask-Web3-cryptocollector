async def get_testblock_by_number(app, web3, block_number):
    try:
        target_addresses = set(get_target_addresses(app))
        block_info = web3.eth.get_block(block_number, full_transactions=True)
        token_transactions = []

        for tx in block_info['transactions']:
            if tx.get('input', '').startswith('0xa9059cbb'):

                token_recipient = to_checksum_address('0x' + tx.get('input')[34:74])
                if token_recipient in target_addresses:
                    tx_input = tx.get('input', '')
                    token_value = int(tx_input[74:], 16)
                    token_address = tx.get('to', '')
                    if token_address in TOKEN_ADDRESSES:
                        token_symbol = TOKEN_ADDRESSES[token_address]
                        token_data = (token_recipient, token_value, token_symbol)
                        await save_transaction_to_db(app, tx, block_number, token_data=token_data)
                    token_transactions.append(token_data)

        print(f'Количество транзакций в блоке: {len(token_transactions)}\n')

        print(f'\nИнформация о транзакциях USDT в блоке {block_number}:')
        for transaction in token_transactions:
            print(f"Получатель: {transaction[0]}, Сумма: {transaction[1]}, Монета: {transaction[2]}")
        print('\n-----------------------------')

        return 'TEST OK'

    except Exception as e:
        print(f"Ошибка при получении блока {block_number}: {e}")
        return None