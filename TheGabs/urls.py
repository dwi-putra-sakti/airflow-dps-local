"""TheGabs URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/3.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  path('', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  path('', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, path
    2. Add a URL to urlpatterns:  path('blog/', include('blog.urls'))
"""
from django.contrib import admin
from django.urls import path
from SalesAndStocks.views import StockAndSalesAPIView
from Stocks.views import StocksAPIView
from Sales.views import SalesAPIView
from SalesThru.views import SalesThruAPIView
from CostAccounting.views import CostAccountingAPIView
from StockDashboard.views import StockDashboardAPIView
from DashboardStatusStock.views import DashboardStatusStockAPIView
from StockMonitoring.views import StockMonitoringAPIView
from ReturMutasi.views import ReturMutasiAPIView

urlpatterns = [
    path('admin/', admin.site.urls),
    path('api/<str:data_type>', StockAndSalesAPIView.as_view(), name='stock-list'),
    path('api/<str:data_type>/<str:request_type>/', StocksAPIView.as_view(), name='list-toko'),
    path('api/<str:data_type>/<str:request_type>/<str:input_value>', StocksAPIView.as_view(), name='stock-by-filter'),
    path('sales/<str:data_type>', SalesAPIView.as_view(), name='sales'),
    path('salesThru/<str:data_type>/', SalesThruAPIView.as_view(), name='sales-thru'),
    path('api/costAccounting/', CostAccountingAPIView.as_view(), name='report'),
    path('api/stockDasboard/<str:input_value>', StockDashboardAPIView.as_view(), name='report'),
    path('api/StockStatus/', DashboardStatusStockAPIView.as_view(), name='report'),
    path('api/accountReceivable/', StockMonitoringAPIView.as_view(), name='AR-report'),
    path('api/returMutasi/<str:input_value>', ReturMutasiAPIView.as_view(), name='Mutasi-report'),
]
