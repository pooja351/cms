from vendors import Vendors
from partners import Partners
from categories import Categories
from inventory import inventory_operations
from boms import Boms
from clients import Clients
from purchase_return import PurchaseReturn
from purchase_order import PurchaseOrder
from boards import Boards
from Forecast import ForcastPurchaseOrder
from invoice import Invoice
from service_order import ServiceOrder
from proforma_invoice import ProformaInvoice
from roles import UserRoles
from quotations import quotation
from users import Users

# from users_and_roles import Users_and_roles
def route_function(request_data,path):
    print(path)
    match path:
        # case "/validateUser":
        #     return Users_and_roles.validateUser(request_data)
        # case "/getUsers":
        #     return Users_and_roles.getUsers(request_data)
        # """-------------------------------------------------------------------------------"""
        # """---------------------------PATHS FOR CATEGORY APIS-----------------------------"""
        # """-------------------------------------------------------------------------------"""
        case '/CmsCategoryAddMetadata':
            print("--------------------------categrdkjnbfcdskjvdslkjaf------------------------")
            return Categories.CmsCategoryAddMetadata(request_data)
        case "/CmsSubCategoriesGetByCategoryName":
            return Categories.CmsSubCategoriesGetByCategoryName(request_data)
        case '/cmsCategoryDelete':
            return Categories.cmsCategoryDelete(request_data)
        case '/CmsCategoryEditMetadata':
            return Categories.CmsCategoryEditMetadata(request_data)
        case '/CmsCategoryGetMetadata':
            return Categories.CmsCategoryGetMetadata(request_data)
        case '/cmsCategoriesGetAllCategoresByDepartment':
            return Categories.cmsCategoriesGetAllCategoresByDepartment(request_data)
        case '/CmsCategoryReplaceImage':
            return Categories.CmsCategoryReplaceImage(request_data)
 
        # case '/cmsCategoriesGetAllCategoresByDepartment':
        #     return Categories.cmsCategoriesGetAllCategoresByDepartment(request_data)
       
        # """-------------------------------------------------------------------------------"""
        # """----------------------------PATH FOR COMPONENT APIS----------------------------"""
        # """-------------------------------------------------------------------------------"""
        case '/CmsInventoryCreateComponent':
            return inventory_operations.CmsInventoryCreateComponent(request_data)
        case "/cmsInventoryGetAllComponentsForCategory":
            return inventory_operations.cmsInventoryGetAllComponentsForCategory(request_data)
        case "/CmsInventoryGetAllData":
            return inventory_operations.CmsInventoryGetAllData(request_data)
        case "/CmsInventoryEditDetails":
            return inventory_operations.CmsInventoryEditDetails(request_data)
        case "/CmsInventoryDeleteComponent":
            return inventory_operations.CmsInventoryDeleteComponent(request_data)
        case "/CmsInventoryUploadCsv":
            return inventory_operations.CmsInventoryUploadCsv(request_data)
        case"/CmsInventoryGeneratePtgPartNumber":
            return inventory_operations.CmsInventoryGeneratePtgPartNumber(request_data)
        case '/cmsComponentReplacementPart':
            return inventory_operations.cmsComponentReplacementPart(request_data)
        case '/cmsComponentGlobalSearch':
            return inventory_operations.cmsComponentGlobalSearch(request_data)
        case "/ComponentActivityDetails":
            return inventory_operations.ComponentActivityDetails(request_data)
        case "/CmsInventoryUpdateStock":
            return inventory_operations.CmsInventoryUpdateStock(request_data)
        case "/CmsInventoryGetInwardIdsForDamaged":
            return inventory_operations.CmsInventoryGetInwardIdsForDamaged(request_data)
       
        # """-------------------------------------------------------------------------------"""
        # """-------------------------------PATH FOR BOM APIS-------------------------------"""
        # """-------------------------------------------------------------------------------"""
        case '/CmsBomCreate':
            return Boms.CmsBomCreate(request_data)
        case "/cmsDeleteBom":
            return Boms.cmsDeleteBom(request_data)
        case "/CmsBomEdit":
            return Boms.CmsBomEdit(request_data)
        case "/CmsBomGetAllData":
            return Boms.CmsBomGetAllData(request_data)
        case "/cmsbomstatusupdate":
            return Boms.cmsbomstatusupdate(request_data)
        case "/cmsbomstatus":
            return Boms.cmsbomstatus(request_data)
        case "/CmsBomGetInnerDetails":
            return Boms.CmsBomGetInnerDetails(request_data)
        case "/get_bom_details":
            return Boms.CmsBomGetDetailsByName(request_data)
        case "/CmsBomGetDetailsByName":
            return Boms.CmsBomGetDetailsByName(request_data)
        case "/cmsBomOutwardInfoSaveAssignToBoxBuilding2":
            return Boms.cmsBomOutwardInfoSaveAssignToBoxBuilding2(request_data)
        case "/cmsBomPriceBom":
            return Boms.cmsBomPriceBom(request_data)
        case "/cmsBomGetPriceBomDetailsById":
            return Boms.cmsBomGetPriceBomDetailsById(request_data)
        case "/bomAssignToVendor":
            return Boms.bomAssignToVendor(request_data)
        case "/cmsBomGetFinalProductInfo":
            return Boms.cmsBomGetFinalProductInfo(request_data)
        case "/CmsInventorySearchSujjestion":
            return Boms.CmsInventorySearchSujjestion(request_data)
        case "/CmsInventorySearchComponent":
            return Boms.CmsInventorySearchComponent(request_data)
        case "/CmsVendorGetAllData":
            return Boms.CmsVendorGetAllData(request_data)
        case "/CmsBomGetAllOutwardList":
            return Boms.CmsBomGetAllOutwardList(request_data)
        case "/CmsOutwardGetBalanceComponentDetails":
            return Boms.CmsOutwardGetBalanceComponentDetails(request_data)
        case "/CmsBomGetEmsOutwardDetailsbyId":
            return Boms.CmsBomGetEmsOutwardDetailsbyId(request_data)
        case "/CmsEmsSendBalanceKitCreate":
            return Boms.CmsEmsSendBalanceKitCreate(request_data)
        case "/cmsFinalProductCreateInPartners":
            return Boms.cmsFinalProductCreateInPartners(request_data)
        case "/cmsGetFinalProductInPartners":
            return Boms.cmsGetFinalProductInPartners(request_data)
        case "/cmsCreateClientInBom":
            return Boms.cmsCreateClientInBom(request_data)
        case "/cmsGetAgainstPo":
            return Boms.cmsGetAgainstPo(request_data)
        case "/cmsAssignToEMSGetPartnersID":
            return Boms.cmsAssignToEMSGetPartnersID(request_data)
        case "/cmsAssignToEMS":
            return Boms.cmsAssignToEMS(request_data)
        case "/cmsGetEmsUploadDocs":
            return Boms.cmsGetEmsUploadDocs(request_data)
        case "/cmsGetAssignToEMSDocuments":
            return Boms.cmsGetAssignToEMSDocuments(request_data)
        case "/cmsClientAgainstPO":
            return Boms.cmsClientAgainstPO(request_data)
        case "/cmsUpdateSaveEMSDoc":
            return Boms.cmsUpdateSaveEMSDoc(request_data)
        case '/cmsBomFinalProductFilter':
            return Boms.cmsBomFinalProductFilter(request_data)
        case '/FinalProductInternalFilterSave':
            return Boms.FinalProductInternalFilterSave(request_data)
        case '/FinalProductReuploadOfProducts':
            return Boms.FinalProductReuploadOfProducts(request_data)
        case '/cmsGetFinalProductDoc':
            return Boms.cmsGetFinalProductDoc(request_data)


        
        
       
        # """-------------------------------------------------------------------------------"""
        # """---------------------------PATH FOR RETURN PO APIS-----------------------------"""
        # """-------------------------------------------------------------------------------"""
        case "/cmsPurchaseReturnGetOrderId":
            return PurchaseReturn.cmsPurchaseReturnGetOrderId(request_data)
        case "/cmsPurchaseReturnGetInwardId":
            return PurchaseReturn.cmsPurchaseReturnGetInwardId(request_data)
        case "/cmsGetComponentDetailsInsidePurchaseReturnModified":
            return PurchaseReturn.cmsGetComponentDetailsInsidePurchaseReturnModified(request_data)
        case "/CmsPurchaseReturnCreateModified":
            return PurchaseReturn.CmsPurchaseReturnCreateModified(request_data)
        case "/cmsPurchaseReturnGetInternalDetails":
            return PurchaseReturn.cmsPurchaseReturnGetInternalDetails(request_data)
        case "/cmsPurchaseReturnEditGetDetails":
            return PurchaseReturn.cmsPurchaseReturnEditGetDetails(request_data)
        case "/cmsPurchaseReturnEdit":
            return PurchaseReturn.cmsPurchaseReturnEdit(request_data)
        case "/cmsPurchaseOrderGetPurchaseReturnList":
            return PurchaseReturn.cmsPurchaseOrderGetPurchaseReturnList(request_data)
        case '/cmsPurchaseOrderGetPurchaseReturnDetails':
            return PurchaseReturn.cmsPurchaseOrderGetPurchaseReturnDetails(request_data)
        case '/cmsNewPurchaseReturnGetOrderId':
            return PurchaseReturn.cmsNewPurchaseReturnGetOrderId(request_data)
        case '/cmsNewPurchaseReturnGetInwardId':
            return PurchaseReturn.cmsNewPurchaseReturnGetInwardId(request_data)
        case '/cmsNewGetComponentDetailsInsidePurchaseReturnModified':
            return PurchaseReturn.cmsNewGetComponentDetailsInsidePurchaseReturnModified(request_data)
       
        case "/CmsNewPurchaseReturnCreate":
            return PurchaseReturn.CmsNewPurchaseReturnCreate(request_data)
        case "/cmsNewPurchaseOrderGetPurchaseReturnList":
            return PurchaseReturn.cmsNewPurchaseOrderGetPurchaseReturnList(request_data)
        case "/cmsGetPdfNewPurchaseOrderReturn":
            return PurchaseReturn.cmsGetPdfNewPurchaseOrderReturn(request_data)
        case "/cmsPurchaseReturnUpdateCommentsAndAttachments":
            return PurchaseReturn.cmsPurchaseReturnUpdateCommentsAndAttachments(request_data)
        case "/cmsGetCommentsAndAttachmentsForPurchaseReturn":
            return PurchaseReturn.cmsGetCommentsAndAttachmentsForPurchaseReturn(request_data)
        case "/cmsNewPurchaseReturnEdit":
            return PurchaseReturn.cmsNewPurchaseReturnEdit(request_data)
        case "/cmsNewPurchaseReturnEditGetDetails":
            return PurchaseReturn.cmsNewPurchaseReturnEditGetDetails(request_data)
        case "/cmsNewPurchaseReturnEdit":
            return PurchaseReturn.cmsNewPurchaseReturnEdit(request_data)
        case "/cmsNewPurchaseReturnEditGetDetails":
            return PurchaseReturn.cmsNewPurchaseReturnEditGetDetails(request_data)

        
        
        
        
        # """-------------------------------------------------------------------------------"""
        # """-----------------------------PATH FOR VENDOR APIS------------------------------"""
        # """-------------------------------------------------------------------------------"""
        case '/cms_vendor_create':
            return Vendors.CmsVendorCreate(request_data)
        case '/CmsVendorGetAllData':
            return Vendors.CmsVendorGetAllData(request_data)
        case '/cms_edit_vendor_details':
            return Vendors.cmsEditVendorDetails(request_data)
        case '/cmsget_all_vendors':
            return Vendors.cmsgetAllVendors(request_data)
        case '/CmsVendorAddRating':
            return Vendors.CmsVendorAddRating(request_data)
        case '/Cms_vendor_get_detailsByName':
            return Vendors.CmsVendorGetDetailsByName(request_data)
        case '/CmsVendorGetDetailsById':
            return Vendors.CmsVendorGetDetailsById(request_data)
        case '/CmsVendorUpdateStatus':
            return Vendors.CmsVendorUpdateStatus(request_data)
        case '/cmsVendorsGetNamesAndIds':
            return Vendors.cmsVendorsGetNamesAndIds(request_data)
        case '/CmsVendorGetAllDataDetails':
            return Vendors.CmsVendorGetAllDataDetails(request_data)
        case '/cmsVendorsGetNamesAndIds':
            return Vendors.cmsVendorsGetNamesAndIds(request_data)
 
        # """-------------------------------------------------------------------------------"""
        # """-----------------------------PATH FOR PARTNER APIS------------------------------"""
        # """-------------------------------------------------------------------------------"""
        case '/CmsPartnerCreate':
            return Partners.CmsPartnerCreate(request_data)
        case '/cmsPartnersGetOutwardList':
            return Partners.cmsPartnersGetOutwardList(request_data)
        case '/cmsPartnerGetStock':
            return Partners.cmsPartnerGetStock(request_data)
        case '/cmsPartnerEMSUpdateStockFetch':
            return Partners.cmsPartnerEMSUpdateStockFetch(request_data)
        case '/cmsPartnerEMSUpdateStockSaveComponents':
            return Partners.cmsPartnerEMSUpdateStockSaveComponents(request_data)
        case '/cmsPartnerUpdateStockBOMList':
            return Partners.cmsPartnerUpdateStockBOMList(request_data)
        case '/cmsPartnerBBUpdateStockSaveComponents':
            return Partners.cmsPartnerBBUpdateStockSaveComponents(request_data)
        case '/cmsPartnerBBUpdateStockFetch':
            return Partners.cmsPartnerBBUpdateStockFetch(request_data)
        case '/cmsPartnerGetStockBoxbuilding':
            return Partners.cmsPartnerGetStockBoxbuilding(request_data)
        case '/CmsGetPartnerDetails':
            return Partners.CmsGetPartnerDetails(request_data)
        case '/cmsGetPartnerDC':
            return Partners.cmsGetPartnerDC(request_data)
        case '/cmsPartnerInStock':
            return Partners.cmsPartnerInStock(request_data)
        case '/cmsGetDCIDForPartnerInvoice':
            return Partners.cmsGetDCIDForPartnerInvoice(request_data)
        case '/cmsCreatePartnerInvoice':
            return Partners.cmsCreatePartnerInvoice(request_data)
        # """-------------------------------------------------------------------------------"""
        # """-----------------------------PATH FOR CLIENT APIS------------------------------"""
        # """-------------------------------------------------------------------------------"""
        case '/create_client':
            return Clients.CmsClientCreate(request_data)
        case '/edit_client':
            return Clients.cmsClientEditDetails(request_data)
        case '/client_upload_po':
            return Clients.CmsClientsUploadPo(request_data)
        case '/client_search_add_bom':
            return Clients.CmsClientSearchAddBom(request_data)
        case '/client_get_all_data':
            return Clients.cmsClientsGetAll(request_data)
        case '/client_bom_search_suggestion':
            return Clients.CmsClientBomSearchSuggestion(request_data)
        case '/client_get_inner_details_by_id':
            return Clients.cmsClientGetInnerDetailsById(request_data)
        case '/cmsAssignClientInBom':
            return Clients.cmsAssignClientInBom(request_data)
        case '/cmsClientAssignDoc':
            return Clients.cmsClientAssignDoc(request_data)
        case '/cmsGetClientAssignPoDetails':
            return Clients.cmsGetClientAssignPoDetails(request_data)
        case '/cmsStatusClientAssignPoDetails':
            return Clients.cmsStatusClientAssignPoDetails(request_data)
        case '/assign_to_boxbuilding':
            return Boards.cmsBomOutwardInfoGetAssignToBoxBuilding(request_data)
        case '/assign_to_boxbuilding2':
            return Boards.cmsBomOutwardInfoGetAssignToBoxBuilding2(request_data)
        case '/save_assign_to_boxbuilding':
            return Boards.cmsBomOutwardInfoSaveAssignToBoxBuilding(request_data)
        case '/partner_send_box_building':
            return Boards.cmsPartnerSendBoxBuildingInfo(request_data)
        case '/save_assign_to_boxbuilding2':
            return Boards.cmsBomOutwardInfoSaveAssignToBoxBuilding2(request_data)
        case '/partner_get_send_boards':
            return Boards.cmsPartnerGetSendBoards(request_data)
        case '/partner_send_boards':
            return Boards.cmsPartnerSendBoards(request_data)
        case '/cmsPartner_Save_Boards_Filter_Save':
            return Boards.cmsPartnerSaveBoardsFilterSave(request_data)
        case '/cmsBomGetSendBoards':
            return Boards.cmsBomGetSendBoards(request_data)
        case '/cmsBoxbuildingDocGet':
            return Boards.cmsBoxbuildingDocGet(request_data)
        case '/cmsBoardBulkUpload':
            return Boards.cmsBoardBulkUpload(request_data)
        case '/cmsFinalProductBulkUpload':
            return Boards.cmsFinalProductBulkUpload(request_data)
        
        case '/cmsBoardsBoxBuildingStatusUpdate':
            return Boards.cmsBoardsBoxBuildingStatusUpdate(request_data)

        

        
        # """-------------------------------------------------------------------------------"""
        # """--------------------------PATH FOR PURCHASE ORDER APIS-------------------------"""
        # """-------------------------------------------------------------------------------"""
        case '/CmsPurchaseOrderCreate':
            return PurchaseOrder.CmsPurchaseOrderCreate(request_data)
        case '/CmsPurchaseOrderEdit':
            return PurchaseOrder.CmsPurchaseOrderEdit(request_data)
        case'/CmsPurchaseOrderEditGet':
            return PurchaseOrder.CmsPurchaseOrderEditGet(request_data)
        case'/CmsPurchaseOrderGetVendor':
            return PurchaseOrder.CmsPurchaseOrderGetVendor(request_data)
        case '/cmsPurchaseOrderGetAllData':
            return PurchaseOrder.cmsPurchaseOrderGetAllData(request_data)
        case '/cmsPurchaseOrderGetVendorDetailsById':
            return PurchaseOrder.cmsPurchaseOrderGetVendorDetailsById(request_data)
        case '/cmsPurchaseOrderGetComponentsById':
            return PurchaseOrder.cmsPurchaseOrderGetComponentsById(request_data)
        case '/CmsPurchaseOrderGetOtherInfo':
            return PurchaseOrder.CmsPurchaseOrderGetOtherInfo(request_data)
        case '/CmsPurchaseOrderGetBankDetails':
            return PurchaseOrder.CmsPurchaseOrderGetBankDetails(request_data)
        case '/CmsPurchaseOrderGetDocumentsById':
            return PurchaseOrder.CmsPurchaseOrderGetDocumentsById(request_data)
        case '/CmsPurchaseOrderGateEntryCreate':
            return PurchaseOrder.CmsPurchaseOrderGateEntryCreate(request_data)
        case '/cmsPurchaseOrderGateEntryGetDetails':
            return PurchaseOrder.cmsPurchaseOrderGateEntryGetDetails(request_data)
        case '/cmsPurchaseOrderQaTestGetDetails':
            return PurchaseOrder.cmsPurchaseOrderQaTestGetDetails(request_data)
        case '/cmsPurchaseOrderSaveQATest':
            return PurchaseOrder.cmsPurchaseOrderSaveQATest(request_data)
        case "/CmsInventorySearchSuggestionInPO":
            return PurchaseOrder.CmsInventorySearchSuggestionInPO(request_data)
        case "/CmsInventorySearchComponentInPO":
            return PurchaseOrder.CmsInventorySearchComponentInPO(request_data)
        case "/CmsInventorySearchComponentInDC":
            return PurchaseOrder.CmsInventorySearchComponentInDC(request_data)
        case "/cmsPurchaseOrderInwardGetAllDetailsForModal":
            return PurchaseOrder.cmsPurchaseOrderInwardGetAllDetailsForModal(request_data)
        case "/cmsPurchaseOrderInwardCategoryInfoGetDetails":
            return PurchaseOrder.cmsPurchaseOrderInwardCategoryInfoGetDetails(request_data)
        case "/cmsPurchaseOrdersInwardGetDetailsbyId":
            return PurchaseOrder.cmsPurchaseOrdersInwardGetDetailsbyId(request_data)
        case '/CmsPurchaseOrderGetStatus':
            return PurchaseOrder.CmsPurchaseOrderGetStatus(request_data)
        case '/cmsPurchaseOrderGetPurchaseList':
            return PurchaseOrder.cmsPurchaseOrderGetPurchaseList(request_data)
        case "/CmsPurchaseOrderSaveInward":
            return PurchaseOrder.CmsPurchaseOrderSaveInward(request_data)
        case "/CmsPurchaseOrderGetStatus":
            return PurchaseOrder.CmsPurchaseOrderGetStatus(request_data)
        case "/CmsPurchaseOrderGetVendorDetails":
            return PurchaseOrder.CmsPurchaseOrderGetVendorDetails(request_data)
        # case "/CmsSearchSuggestionInPO":
        #     return PurchaseOrder.CmsSearchSuggestionInPO(request_data)
        # case "/CmsSearchComponentInPO":
        #     return PurchaseOrder.CmsSearchComponentInPO(request_data)
        case  "/CmsActiveClientPurchaseorder":
            return PurchaseOrder.CmsActiveClientPurchaseorder(request_data)
        case "/CmsCreatePurchaseOrderSaveDraft":
            return PurchaseOrder.CmsCreatePurchaseOrderSaveDraft(request_data)
        case "/cmsGetDraftList":
            return PurchaseOrder.cmsGetDraftList(request_data)
        case "/cmsGetEditDraft":
            return PurchaseOrder.cmsGetEditDraft(request_data)
        case "/cmsEditPODraft":
            return PurchaseOrder.cmsEditPODraft(request_data)
        case "/cmsDeleteDraft":
            return PurchaseOrder.cmsDeleteDraft(request_data)
        case "/deleteDraft":
            return PurchaseOrder.deleteDraft(request_data)
        case "/CmsNewPurchaseOrderCreate":
            return PurchaseOrder.CmsNewPurchaseOrderCreate(request_data)
        case "/CmsNewCreatePurchaseOrderSaveDraft":
            return PurchaseOrder.CmsNewCreatePurchaseOrderSaveDraft(request_data)
        case "/cmsGetEditPurchaseOrder":
            return PurchaseOrder.cmsGetEditPurchaseOrder(request_data)
        case "/cmsGetAlldetailsForDocumentNumber":
            return PurchaseOrder.cmsGetAlldetailsForDocumentNumber(request_data)
        case "/cmsGetPOGateEntry":
            return PurchaseOrder.cmsGetPOGateEntry(request_data)
        case "/cmsPOCardDetails":
            return PurchaseOrder.cmsPOCardDetails(request_data)
        case "/cmsCreatePOGateEntry":
            return PurchaseOrder.cmsCreatePOGateEntry(request_data)
        case "/cmsPOGetComponentsById":
            return PurchaseOrder.cmsPOGetComponentsById(request_data)
        case "/getGateEntryPopUp":
            return PurchaseOrder.getGateEntryPopUp(request_data)
        case "/getInwardPopUp":
            return PurchaseOrder.getInwardPopUp(request_data)
        case "/getIQCPopUp":
            return PurchaseOrder.getIQCPopUp(request_data)
        case "/saveCommentsForPurchaseOrder":
            return PurchaseOrder.saveCommentsForPurchaseOrder(request_data)

        case "/cmsAddCommentsAndAttachmentsForPurchaseOrder":
            return PurchaseOrder.cmsAddCommentsAndAttachmentsForPurchaseOrder(request_data)
        case "/cmsGetCommentsAndAttachmentsForPurchaseOrder":
            return PurchaseOrder.cmsGetCommentsAndAttachmentsForPurchaseOrder(request_data)
    
        case "/cmsGetGateEntryIdForNewPo":
            return PurchaseOrder.cmsGetGateEntryIdForNewPo(request_data)
    
        case "/cmsNewPurchaseOrderQaTestGetDetails":
            return PurchaseOrder.cmsNewPurchaseOrderQaTestGetDetails(request_data)
    
        case "/cmsNewPurchaseOrderSaveQATest":
            return PurchaseOrder.cmsNewPurchaseOrderSaveQATest(request_data)
        case "/cmsNewInwardGetQatestId":
            return PurchaseOrder.cmsNewInwardGetQatestId(request_data)
        case "/cmsNewPurchaseOrdersInwardGetById":
            return PurchaseOrder.cmsNewPurchaseOrdersInwardGetById(request_data)
        case "/CmsNewPurchaseOrderSaveInward":
            return PurchaseOrder.CmsNewPurchaseOrderSaveInward(request_data)
        
        case "/cmsUpdateProformaInvoicePurchaseOrders":
            return PurchaseOrder.cmsUpdateProformaInvoicePurchaseOrders(request_data)
        
        case "/cmsUpdateProformaInvoiceCommentPurchaseOrders":
            return PurchaseOrder.cmsUpdateProformaInvoiceCommentPurchaseOrders(request_data)
        
        case "/cmsUpdateEditPO":
            return PurchaseOrder.cmsUpdateEditPO(request_data)
        
        case "/CmsActiveForcastPurchaseorder":
            return PurchaseOrder.CmsActiveForcastPurchaseorder(request_data)
        
        
        
        
        

        
        
        


        
       
        

        
    
        # """-------------------------------------------------------------------------------"""
        # """--------------------------PATH FOR Inventory  APIS-------------------------"""
        # """-------------------------------------------------------------------------------"""
 
 
 
        case '/cmsInventoryClassificationPartsCount':
            return inventory_operations.cmsInventoryClassificationPartsCount(request_data)
        case '/cmsClassificationPartsSearchSuggestion':
            return inventory_operations.cmsClassificationPartsSearchSuggestion(request_data)  
        case '/cmsInventoryGetAllBomid':
            return inventory_operations.cmsInventoryGetAllBomid(request_data)  
        case '/cmsGetInventoryStockDetails':
            return inventory_operations.cmsGetInventoryStockDetails(request_data)
        case '/cmsVendorGetByComponentId':
            return inventory_operations.cmsVendorGetByComponentId(request_data)
        case '/cmsVendorGetOrderDetails':
            return inventory_operations.cmsVendorGetOrderDetails(request_data)
        case '/cmsVendorGetTopFive':
            return inventory_operations.cmsVendorGetTopFive(request_data)
        case '/cmsVendorGetInnerOrderDetails':
            return inventory_operations.cmsVendorGetInnerOrderDetails(request_data)
        case '/cmsComponentGetRackDetails':
            return inventory_operations.cmsComponentGetRackDetails(request_data)  
        case '/cmsGetInventoryStockDetailsModified':
            return inventory_operations.cmsGetInventoryStockDetailsModified(request_data)
        case '/cmsInventoryGetInnerBom':
            return inventory_operations.cmsInventoryGetInnerBom(request_data)
        case '/get_inventory_stock_detailsmodified':
            return inventory_operations.get_inventory_stock_detailsmodified(request_data)
        case '/CmsInventoryGetKitsCount':
            return inventory_operations.CmsInventoryGetKitsCount(request_data)
        case '/CmsVendorGetDetailsByName':
            return Vendors.CmsVendorGetDetailsByName(request_data)
###################Forecast##############
 
        case "/CmsCreateForcastPurchaseOrder":
            return ForcastPurchaseOrder.CmsCreateForcastPurchaseOrder(request_data)
        case "/CmsGetClientIdForcastPurchaseOrderDetails":
            return ForcastPurchaseOrder.CmsGetClientIdForcastPurchaseOrderDetails(request_data)
        case "/cmsForecastPOGetBomsForClientName":
            return ForcastPurchaseOrder.cmsForecastPOGetBomsForClientName(request_data)
        case "/CmsForcastPurchaseOrderUploadPO":
            return ForcastPurchaseOrder.CmsForcastPurchaseOrderUploadPO(request_data)
        case "/CmsSaveDraftForecastPurchaseOrder":
            return ForcastPurchaseOrder.CmsSaveDraftForecastPurchaseOrder(request_data)
        case "/CmsForecastPOGetBomPriceForBomName":
            return ForcastPurchaseOrder.CmsForecastPOGetBomPriceForBomName(request_data)
        case "/CmsForcastPurchaseOrderPostComment":
            return ForcastPurchaseOrder.CmsForcastPurchaseOrderPostComment(request_data)
        case "/CmsGetInnerForcastPurchaseOrderDetails":
            return ForcastPurchaseOrder.CmsGetInnerForcastPurchaseOrderDetails(request_data)
        case "/CmsGetForcastPurchaseOrderDetailsList":
            return ForcastPurchaseOrder.CmsGetForcastPurchaseOrderDetailsList(request_data)
               
        case "/CmsCreateClientForcastPurchaseOrder":
            return ForcastPurchaseOrder.CmsCreateClientForcastPurchaseOrder(request_data)

        case "/CmsGetClientForcastPurchaseOrderDetailsList":
            return ForcastPurchaseOrder.CmsGetClientForcastPurchaseOrderDetailsList(request_data)
        case "/cmsGetPurchaseOrderApprovals":
            return ForcastPurchaseOrder.cmsGetPurchaseOrderApprovals(request_data)
        case "/cmsGetPurchaseOrderApprovalsDetails":
            return ForcastPurchaseOrder.cmsGetPurchaseOrderApprovalsDetails(request_data)
        
        case "/cmsGetDraftList":
            return ForcastPurchaseOrder.cmsGetDraftList(request_data)
        
        case "/cmsEditPODraft":
            return ForcastPurchaseOrder.cmsEditPODraft(request_data)
        case "/getPurchaseOrderPdfDetails":
            return ForcastPurchaseOrder.getPurchaseOrderPdfDetails(request_data)
        
        case "/CmsEditUpdateForcastPurchaseOrder":
            return ForcastPurchaseOrder.CmsEditUpdateForcastPurchaseOrder(request_data)
        
        case "/CmsEditGetForecastPurchaseOrder":
            return ForcastPurchaseOrder.CmsEditGetForecastPurchaseOrder(request_data)
        
        case "/CmsSaveDraftForecastPurchaseOrder":
            return ForcastPurchaseOrder.CmsSaveDraftForecastPurchaseOrder(request_data)
        
        case "/CmsDraftUpdateEditForecastPurchaseOrder":
            return ForcastPurchaseOrder.CmsDraftUpdateEditForecastPurchaseOrder(request_data)
        case "/CmsGetPoDetailsForTransaction":
            return ForcastPurchaseOrder.CmsGetPoDetailsForTransaction(request_data)
        case "/saveCommentsForTransactionsInChildPo":
            return ForcastPurchaseOrder.saveCommentsForTransactionsInChildPo(request_data)
        

        
        


        
        









        case "/CmsGetForcastPurchaseOrderDetailsList1":
            return ForcastPurchaseOrder.CmsGetForcastPurchaseOrderDetailsList1(request_data)
        
        case "/CmsDraftCreateClientForcastPurchaseOrder":
            return ForcastPurchaseOrder.CmsDraftCreateClientForcastPurchaseOrder(request_data)
        case "/CmsDraftClientForcastPurchaseOrderEdit":
            return ForcastPurchaseOrder.CmsDraftClientForcastPurchaseOrderEdit(request_data)
        case "/CmsClientForcastPurchaseOrderEditGet":
            return ForcastPurchaseOrder.CmsClientForcastPurchaseOrderEditGet(request_data)
        
        case "/CmsUpdateClientForcastPurchaseOrderEdit":
            return ForcastPurchaseOrder.CmsUpdateClientForcastPurchaseOrderEdit(request_data)
        
        case "/CmsGetInnerForcastPurchaseOrderDetailsList":
            return ForcastPurchaseOrder.CmsGetInnerForcastPurchaseOrderDetailsList(request_data)
        case "/CmsGetVendorPartnerNameDetailsList":
            return ForcastPurchaseOrder.CmsGetVendorPartnerNameDetailsList(request_data)
        
        case "/CmsActiveForcastChildPurchaseorder":
            return ForcastPurchaseOrder.CmsActiveForcastChildPurchaseorder(request_data)
        
        
        

        

        
        # """-------------------------------------------------------------------------------"""
        # """--------------------------PATH FOR Invoice  APIS-------------------------"""
        # """-------------------------------------------------------------------------------"""

        case "/cmsPOClientList":
            return Invoice.cmsPOClientList(request_data)
        case "/cmsGetClientDetails":
            return Invoice.cmsGetClientDetails(request_data)
        case "/cmsInvoiceSearch":
            return Invoice.cmsInvoiceSearch(request_data)
        case "/cmsInvoiceCreate":
            return Invoice.cmsInvoiceCreate(request_data)
        case "/cmsInvoiceSearchAdd":
            return Invoice.cmsInvoiceSearchAdd(request_data)
        case "/cmsGetEditInvoice":
            return Invoice.cmsGetEditInvoice(request_data)
        case "/cmsInvoiceEdit":
            return Invoice.cmsInvoiceEdit(request_data)
        case "/cmsInvoiceSaveDraft":
            return Invoice.cmsInvoiceSaveDraft(request_data)
        case "/cmsInvoiceUpdateDraft":
            return Invoice.cmsInvoiceUpdateDraft(request_data)


        # """-------------------------------------------------------------------------------"""
        # """--------------------------PATH FOR Service APIS-------------------------"""
        # """-------------------------------------------------------------------------------"""

        case "/CmsNewServiceOrderCreate":
            return ServiceOrder.CmsNewServiceOrderCreate(request_data)
        case "/CmsPurchaseOrderGetPartnersDetails":
            return ServiceOrder.CmsPurchaseOrderGetPartnersDetails(request_data)
        case "/CmsGetPartnerNameList":
            return ServiceOrder.CmsGetPartnerNameList(request_data)
        case "/CmsUpdateServiceOrder":
            return ServiceOrder.CmsUpdateServiceOrder(request_data)
        case "/CmsServiceOrderGet":
            return ServiceOrder.CmsServiceOrderGet(request_data)
        case "/CmsDraftServiceOrderCreate":
            return ServiceOrder.CmsDraftServiceOrderCreate(request_data)
        case "/cmsServiceUpdateDraft":
            return ServiceOrder.cmsServiceUpdateDraft(request_data)
        case "/purchaseOrderCancel":
            return ServiceOrder.purchaseOrderCancel(request_data)
        case "/getAllCancelledOrders":
            return ServiceOrder.getAllCancelledOrders(request_data)
        case "/cmsDeliveryChallanSOGet":
            return ServiceOrder.cmsDeliveryChallanSOGet(request_data)
        case "/getServiceOrderDetails":
            return ServiceOrder.getServiceOrderDetails(request_data)
        case "/cmsDeliveryChallanSOSave":
            return ServiceOrder.cmsDeliveryChallanSOSave(request_data)
        case "/cmsSaveCommentsForServiceOrder":
            return ServiceOrder.cmsSaveCommentsForServiceOrder(request_data)
        case "/cmsGetServiceOrderCardDetails":
            return ServiceOrder.cmsGetServiceOrderCardDetails(request_data)
        case "/cmsGetDCCards":
            return ServiceOrder.cmsGetDCCards(request_data)
        case "/cmsDCBOMGetComponents":
            return ServiceOrder.cmsDCBOMGetComponents(request_data)
        case "/cmsgetDCID":
            return ServiceOrder.getDCID(request_data)
        case "/getDCDataByDCID":
            return ServiceOrder.getDCDataByDCID(request_data)
        case "/CmsServiceOrderSaveInward":
            return ServiceOrder.CmsServiceOrderSaveInward(request_data)
        case "/cmsCreateServiceOrderGateEntry":
            return ServiceOrder.cmsCreateServiceOrderGateEntry(request_data)
        case "/cmsDCGetComponentsById":
            return ServiceOrder.cmsDCGetComponentsById(request_data)
        
        case "/cmsGetServiceOrderGateEntryId":
            return ServiceOrder.cmsGetServiceOrderGateEntryId(request_data)
        
        case "/cmsServiceOrderQaTestGetDetails":
            return ServiceOrder.cmsServiceOrderQaTestGetDetails(request_data)
        
        case "/cmsServiceOrderSaveIQCTest":
            return ServiceOrder.cmsServiceOrderSaveIQCTest(request_data)
        case "/getIQCIds":
            return ServiceOrder.getIQCIds(request_data)
        case "/getIQCDataByID":
            return ServiceOrder.getIQCDataByID(request_data)
        case "/getSOGateEntryPopUp":
            return ServiceOrder.getSOGateEntryPopUp(request_data)
        case "/getSOInwardPopUp":
            return ServiceOrder.getSOInwardPopUp(request_data)
        case "/getSOIQCPopUp":
            return ServiceOrder.getSOIQCPopUp(request_data)
        case "/saveCommentsForServiceOrderGateEntry":
            return ServiceOrder.saveCommentsForServiceOrderGateEntry(request_data)
        case "/cmsSoGetComponentsById":
            return ServiceOrder.CmsSoGetComponentsById(request_data)
        case "/saveSoEolBinaryBuildDetails":
            return ServiceOrder.saveSoEolBinaryBuildDetails(request_data)
        case "/getSoEolBinaryBuildDetails":
            return ServiceOrder.getSoEolBinaryBuildDetails(request_data)

        case "/cmsGetServiceOrderGateEntry":
            return ServiceOrder.cmsGetServiceOrderGateEntry(request_data)
        case "/cmsPartnersCreateProductionLine":
            return ServiceOrder.cmsPartnersCreateProductionLine(request_data)
        case "/cmsPartnerGetProductionLine":
            return ServiceOrder.cmsPartnerGetProductionLine(request_data)
        #########proforma_invoice########
        
        case "/cmsproformaInvoicegetClientDetails":
            return ProformaInvoice.cmsproformaInvoicegetClientDetails(request_data)
        case "/cmsproformaInvoiceCreate":
            return ProformaInvoice.cmsproformaInvoiceCreate(request_data)
        case "/CmsEditProformaInvoice":
            return ProformaInvoice.CmsEditProformaInvoice(request_data)
        case "/proformaInvoiceEditGet":
            return ProformaInvoice.proformaInvoiceEditGet(request_data)
        case "/cmsproformaInvoiceSaveDraft":
            return ProformaInvoice.cmsproformaInvoiceSaveDraft(request_data)
        case "/CmsDraftEditProformaInvoice":
            return ProformaInvoice.CmsDraftEditProformaInvoice(request_data)
        case "/cmsPOGetProformaInvoice":
            return ProformaInvoice.cmsPOGetProformaInvoice(request_data)
        case "/cmsActiveForcastPurchaseorderDetails":
            return ProformaInvoice.CmsActiveForcastPurchaseorderDetails(request_data)
        case "/cmsGetProformaInvoiceCardDetails":
            return ProformaInvoice.cmsGetProformaInvoiceCardDetails(request_data)
        case "/cmsSaveProformaInvoiceCardDetails":
            return ProformaInvoice.cmsSaveProformaInvoiceCardDetails(request_data)
        case "/cmsSaveFCPOCardDetails":
            return ProformaInvoice.cmsSaveFCPOCardDetails(request_data)
        case "/cmsGetFCPOId":
            return ProformaInvoice.cmsGetFCPOId(request_data)

        
        ###########user_roles#############
        
        case "/createUserRole":
            return UserRoles.createUserRole(request_data)
        case "/getUserRoles":
            return UserRoles.getUserRoles(request_data)
        case "/assignPermissionsToRole":
            return UserRoles.assignPermissionsToRole(request_data)
        case "/getRolePermissions":
            return UserRoles.getRolePermissions(request_data)
        
        
        ###########Quotations###############
        
        case "/getPTGAddress":
            return quotation.getPTGAddress(request_data)
        case "/saveQuotationForm":
            return quotation.saveQuotationForm(request_data)
        case "/getQuotationById":
            return quotation.getQuotationById(request_data)
        case "/updateQuotationForm":
            return quotation.updateQuotationForm(request_data)
        case "/saveDraftQuotationForm":
            return quotation.saveDraftQuotationForm(request_data)
        case "/getDraftQuotationById":
            return quotation.getDraftQuotationById(request_data)
        case "/updateDraftQuotationForm":
            return quotation.updateDraftQuotationForm(request_data)
        

        ############USERS INFO  ##################
        case "/createandsaveuser":
            return Users.createUser(request_data)
        case "/listofusers":
            return Users.getUsers(request_data)
        case "/deleteuser":
            return Users.deleteUser(request_data)
        case "/updateuser":
            return Users.updateUser(request_data)
        case "/getdetailsbyuserid":
            return Users.getUserById(request_data)
        case "/updatetoken":
            return Users.updateUserToken(request_data)
        case "/saveuserlogs":
            return Users.saveUserLoginLog(request_data)
        case "/screenpermissionsbyuser":
            return Users.getUserScreenpermissionsDetails(request_data)
        
        

        



       


        


        
 