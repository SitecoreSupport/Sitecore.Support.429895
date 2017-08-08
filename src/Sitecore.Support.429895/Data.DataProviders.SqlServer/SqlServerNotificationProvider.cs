namespace Sitecore.Support.Data.DataProviders.SqlServer
{
  using System;
  using System.Collections.Generic;
  using Sitecore.Data;
  using Sitecore.Data.Clones;
  using Sitecore.Diagnostics;
  using Sitecore.Globalization;

  public class SqlServerNotificationProvider : Sitecore.Data.DataProviders.SqlServer.SqlServerNotificationProvider
  {
    [UsedImplicitly]
    public SqlServerNotificationProvider(string connectionStringName, string databaseName)
      : base(connectionStringName, databaseName)
    {
    }
    
    public override IEnumerable<Notification> GetNotifications(ItemUri itemUri)
    {
      Assert.ArgumentNotNull(itemUri, nameof(itemUri));

      var sql = @"
        SELECT 
          {0}Id{1}, {0}ItemId{1}, {0}Language{1}, {0}Version{1}, {0}Processed{1}, {0}InstanceType{1}, {0}InstanceData{1}, {0}Created{1}  
        FROM 
          {0}Notifications{1} 
          WITH (NOLOCK)
        WHERE 
          {0}Processed{1}={2}processed{3} AND {0}ItemId{1}={2}itemId{3} 
          AND (({0}Language{1}={2}invariantLanguage{3}) OR ({0}Language{1}={2}language{3}) OR ({0}Language{1} IS NULL))
          AND (({0}Version{1}={2}latestVersion{3}) OR ({0}Version{1}={2}version{3}))
        ORDER BY 
          {0}Created{1}
        OPTION 
          (MAXDOP 8)";

      var parameters = new object[]
      {
        "processed", false, 
        "itemId", itemUri.ItemID, 
        "invariantLanguage", Language.Invariant.ToString(),
        "language", itemUri.Language.ToString(), 
        "latestVersion", Sitecore.Data.Version.Latest.Number, 
        "version", itemUri.Version.Number
      };

      return DataApi.CreateObjectReader(sql, parameters, CreateNotification);
    }
    
    public override IEnumerable<Notification> GetNotifications(Type notificationType)
    {
      Assert.ArgumentNotNull(notificationType, nameof(notificationType));

      var sql = @"
        SELECT 
          {0}Id{1}, {0}ItemId{1}, {0}Language{1}, {0}Version{1}, {0}Processed{1}, {0}InstanceType{1}, {0}InstanceData{1}, {0}Created{1}  
        FROM 
          {0}Notifications{1} 
          WITH (NOLOCK)
        WHERE 
          {0}Processed{1}={2}processed{3} AND {0}InstanceType{1}={2}instanceType{3}              
        ORDER BY 
          {0}Created{1}
        OPTION 
          (MAXDOP 8)";

      var parameters = new object[]
      {
        "processed", false, 
        "instanceType", notificationType.AssemblyQualifiedName
      };

      return DataApi.CreateObjectReader(sql, parameters, CreateNotification);
    }
    
    public override IEnumerable<Notification> GetNotifications()
    {
      var sql = @"
        SELECT 
          {0}Id{1}, {0}ItemId{1}, {0}Language{1}, {0}Version{1}, {0}Processed{1}, {0}InstanceType{1}, {0}InstanceData{1}, {0}Created{1} 
        FROM 
          {0}Notifications{1} 
          WITH (NOLOCK) 
        ORDER BY 
          {0}Created{1}
        OPTION 
          (MAXDOP 8)";

      var parameters = new object[] { };

      return DataApi.CreateObjectReader(sql, parameters, CreateNotification);
    }
  }
}