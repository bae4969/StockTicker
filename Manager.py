import Util
import doc.Define as Define
from ApiKoreaInvest import ApiKoreaInvestType as API_KI
from ApiBithumb import ApiBithumbType as API_BH
import time
import tabulate


bh = API_BH(
        Define.SQL_HOST,
        Define.SQL_ID,
        Define.SQL_PW,
        Define.SQL_BH_DB,
        )
ki = API_KI(
	Define.SQL_HOST,
	Define.SQL_ID,
	Define.SQL_PW,
	Define.SQL_KI_DB,
	Define.APP_KEY,
	Define.APP_SECRET,
	)

print("\n---------------------------------------------\n")

while True:
	try:
		find_ret = None
		exe_bool_ret = None
		exe_int_ret = None

		print("Manager > ", end='')
		cmd_args = input().split(' ')
		if cmd_args[0].lower() == "exit":
			break
		
		elif cmd_args[0].lower() == "find":
			if cmd_args[1].lower() == "coin":
				find_ret = bh.FindCoin(cmd_args[2])
			elif cmd_args[1].lower() == "stock":
				find_ret = ki.FindStock(cmd_args[2])
			elif cmd_args[1].lower() == "kr":
				find_ret = ki.FindKrStock(cmd_args[2])
			elif cmd_args[1].lower() == "us":
				find_ret = ki.FindUsStock(cmd_args[2])
			else:
				raise
			
		elif cmd_args[0].lower() == "list":
			cnt = int(cmd_args[2])
			if len(cmd_args) == 3:
				offset = 0
			else:
				offset = int(cmd_args[3])

			if cmd_args[1].lower() == "coin":
				find_ret = bh.FindCoin(cnt, offset)
			elif cmd_args[1].lower() == "stock":
				raise Exception("List command can not use stock option")
			elif cmd_args[1].lower() == "kr":
				find_ret = ki.GetKrStockList(cnt, offset)
			elif cmd_args[1].lower() == "us":
				find_ret = ki.GetUsStockList(cnt, offset)
			else:
				raise

		elif cmd_args[0].lower() == "clear":
			if cmd_args[1].lower() == "coin":
				bh.ClearAllQuery()
			elif (cmd_args[1].lower() == "stock" or
				cmd_args[1].lower() == "kr" or
				cmd_args[1].lower() == "us"):
				ki.ClearAllQuery()
			else:
				raise
			
		elif cmd_args[0].lower() == "reg":
			if cmd_args[1].lower() == "coin":
				find_ret = bh.GetInsertedQueryList()
			elif cmd_args[1].lower() == "stock":
				raise Exception("List command can not use stock option")
			elif (cmd_args[1].lower() == "kr"):
				find_ret = ki.GetInsertedKrQueryList()
			elif (cmd_args[1].lower() == "us"):
				find_ret = ki.GetInsertedUsQueryList()
			else:
				raise

		elif cmd_args[0].lower() == "update":
			if cmd_args[1].lower() == "coin":
				exe_bool_ret = bh.UpdateAllQuery()
			elif (cmd_args[1].lower() == "stock" or
				cmd_args[1].lower() == "kr" or
				cmd_args[1].lower() == "us"):
				exe_bool_ret = ki.UpdateAllQuery()
			else:
				raise
				
		elif cmd_args[0].lower() == "add":
			if cmd_args[1].lower() == "coin":
				exe_int_ret = bh.AddCoinExecutionQuery(cmd_args[2])
			elif (cmd_args[1].lower() == "stock" or
				cmd_args[1].lower() == "kr" or
				cmd_args[1].lower() == "us"):
				exe_int_ret = ki.AddStockExecutionQuery(cmd_args[2])
			else:
				raise
			
		elif cmd_args[0].lower() == "del":
			if cmd_args[1].lower() == "coin":
				bh.DelCoinExecutionQuery(cmd_args[2])
			elif (cmd_args[1].lower() == "stock" or
				cmd_args[1].lower() == "kr" or
				cmd_args[1].lower() == "us"):
				ki.DelStockExecutionQuery(cmd_args[2])
			else:
				raise


		if find_ret != None:
			header = ["Code", "KR Name", "EN Name", "Market"]
			print(tabulate.tabulate(find_ret, headers=header, tablefmt='pretty', showindex=True), end="\n\n")
		
		elif exe_bool_ret != None:
			if exe_bool_ret == True:
				print("Manager > Success to execute")
			else:
				print("Manager > Fail to execute")
				
		elif exe_int_ret != None:
			if exe_int_ret >= 0:
				print("Manager > Success to execute (Count : %d)"%exe_int_ret)
			elif exe_int_ret == -500:
				print("Manager > Invalid input")
			elif exe_int_ret == -501:
				print("Manager > Fail by query limit")
			else:
				print("Manager > Fail to execute")

	except Exception as e:
		print("Manager > Invalid input was detected (%s)"%e.__str__())
		pass


print("---------------------------------------------\n")

