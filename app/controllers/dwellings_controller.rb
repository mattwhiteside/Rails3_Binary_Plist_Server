class DwellingsController < ActionController::Base
   layout 'application'

   respond_to	:xml,:json, :plist	

   OPERATOR_MAP = {'having' => "=", 'having at least' => ">="}


   def index      
     dwellings_table = Arel::Table.new(:dwellings)
     columns = Dwelling.columns.map{|c| c.name} - ["created_at","id","updated_at","state","foreign_database_updated_at"]
     dwellings = params[:type].constantize.select(
        columns
     ).within_box(
        params[:top],params[:bottom],params[:left],params[:right]
     ).created_after(
        Time.now-Dwelling::DECENT_CUTOFF.days
     ).where(
        dwellings_table[:price].lteq params[:max_price]
     ).where(
        "bedrooms #{OPERATOR_MAP[params[:bedroom_operator]]} #{params[:bedrooms]} OR bedrooms IS NULL"
     ).where(
        "bathrooms #{OPERATOR_MAP[params[:bathroom_operator]]} #{params[:bathrooms]} OR bathrooms IS NULL"
     )

     if params[:eid].present?
        dwellings = dwellings.where(dwellings_table[:foreign_database_id].not_in params[:eid])
     end
     respond_with dwellings.order("foreign_database_created_at DESC").limit(params[:limit]).all
   end


end
